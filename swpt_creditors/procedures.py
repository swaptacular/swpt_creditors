from uuid import UUID
from datetime import datetime, date, timedelta, timezone
from typing import TypeVar, Callable, Tuple, List, Optional
from flask import current_app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import tuple_, and_
from sqlalchemy.orm import joinedload, exc
from swpt_lib.utils import Seqnum, increment_seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import Creditor, LedgerEntry, CommittedTransfer, Account, \
    AccountData, AccountConfig, ConfigureAccountSignal, PendingAccountCommit, LogEntry, \
    AccountDisplay, AccountExchange, AccountKnowledge, DirectTransfer, RunningTransfer, \
    MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_SECOND = timedelta(seconds=1)
TD_5_SECONDS = timedelta(seconds=5)
HUGE_NEGLIGIBLE_AMOUNT = 1e30
PENDING_ACCOUNT_COMMIT_PK = tuple_(
    PendingAccountCommit.debtor_id,
    PendingAccountCommit.creditor_id,
    PendingAccountCommit.transfer_number,
)
ACCOUNT_CONFIG_JOIN_CLAUSE = and_(
    AccountConfig.creditor_id == AccountData.creditor_id,
    AccountConfig.debtor_id == AccountData.debtor_id,
)


def init(path_builder, schema_types):
    """"Must be called before using any of the functions in the module."""

    global paths, types
    paths = path_builder
    types = schema_types


class CreditorDoesNotExistError(Exception):
    """The creditor does not exist."""


class CreditorExistsError(Exception):
    """The same creditor record already exists."""


class AccountDoesNotExistError(Exception):
    """The account does not exist."""


class TransferDoesNotExistError(Exception):
    """The transfer does not exist."""


class TransferUpdateConflictError(Exception):
    """The requested transfer update is not possible."""


class TransferCanNotBeCanceledError(Exception):
    """The requested transfer cancellation is not possible."""


class AccountExistsError(Exception):
    """The same account record already exists."""


class ForbiddenAccountCreationError(Exception):
    """The creation of the account is forbidden."""


class AccountsConflictError(Exception):
    """A different account with the same debtor ID already exists."""


@atomic
def get_creditor(creditor_id: int, lock: bool = False) -> Optional[Creditor]:
    if lock:
        creditor = Creditor.lock_instance(creditor_id)
    else:
        creditor = Creditor.get_instance(creditor_id)

    if creditor and creditor.deactivated_at_date is None:
        return creditor

    return None


@atomic
def create_new_creditor(creditor_id: int) -> Creditor:
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    creditor = Creditor(creditor_id=creditor_id)
    db.session.add(creditor)
    try:
        db.session.flush()
    except IntegrityError:
        raise CreditorExistsError()
    return creditor


@atomic
def update_creditor(creditor_id: int) -> Creditor:
    creditor = get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise CreditorDoesNotExistError()

    creditor.creditor_latest_update_id, creditor.creditor_latest_update_ts = _add_log_entry(
        creditor,
        object_type=types.creditor,
        object_uri=paths.creditor(creditorId=creditor_id),
    )
    return creditor


@atomic
def lock_or_create_creditor(creditor_id: int) -> Creditor:
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    creditor = Creditor.lock_instance(creditor_id)
    if creditor is None:
        creditor = Creditor(creditor_id=creditor_id)
        with db.retry_on_integrity_error():
            db.session.add(creditor)
    return creditor


@atomic
def get_log_entries(creditor_id: int, count: int = 1, prev: int = 0) -> Tuple[Creditor, List[LogEntry]]:

    assert count >= 1
    assert 0 <= prev <= MAX_INT64

    creditor = get_creditor(creditor_id)
    if creditor is None:
        raise CreditorDoesNotExistError()

    entries = LogEntry.query.\
        filter(LogEntry.creditor_id == creditor_id).\
        filter(LogEntry.entry_id > prev).\
        order_by(LogEntry.entry_id).\
        limit(count).\
        all()

    return creditor, entries


@atomic
def get_account_debtor_ids(creditor_id: int, count: int = 1, prev: int = None) -> List[int]:
    assert count >= 1
    assert prev is None or MIN_INT64 <= prev <= MAX_INT64

    query = db.session.query(Account.debtor_id).\
        filter(Account.creditor_id == creditor_id).\
        order_by(Account.debtor_id)

    if prev is not None:
        query = query.filter(Account.debtor_id > prev)

    return [t[0] for t in query.limit(count).all()]


@atomic
def get_account(creditor_id: int, debtor_id: int, lock: bool = False, join: bool = False) -> Optional[Account]:
    options = [
        joinedload(Account.knowledge, innerjoin=True),
        joinedload(Account.exchange, innerjoin=True),
        joinedload(Account.display, innerjoin=True),
        joinedload(Account.config, innerjoin=True),
        joinedload(Account.data, innerjoin=True),
    ] if join else []

    if lock:
        account = Account.lock_instance((creditor_id, debtor_id), *options, of=Account)
    else:
        account = Account.get_instance((creditor_id, debtor_id), *options)

    return account


@atomic
def create_account(creditor_id: int, debtor_id: int) -> Account:
    """"Try to create and return a new account.

    May raise `CreditorDoesNotExistError`, `AccountExistsError`, or
    `ForbiddenAccountCreationError`.

    """

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise CreditorDoesNotExistError()

    update_id, update_ts = _add_log_entry(
        creditor,
        object_type=types.account,
        object_uri=paths.account(creditorId=creditor_id, debtorId=debtor_id),
        current_ts=current_ts,
    )
    latest_update = {'latest_update_id': update_id, 'latest_update_ts': update_ts}
    account = Account(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        created_at_ts=current_ts,
        knowledge=AccountKnowledge(**latest_update),
        exchange=AccountExchange(**latest_update),
        display=AccountDisplay(**latest_update),
        config=AccountConfig(**latest_update),
        data=AccountData(
            last_config_ts=update_ts,
            info_latest_update_id=update_id,
            info_latest_update_ts=update_ts,
            ledger_latest_update_id=update_id,
            ledger_latest_update_ts=update_ts,
        ),
        **latest_update,
    )
    db.session.add(account)

    try:
        db.session.flush()
    except IntegrityError:
        db.session.rollback()
        raise AccountExistsError()

    _insert_configure_account_signal(account.config, account.data, account.created_at_ts)

    return account


@atomic
def change_account_config(
        creditor_id: int,
        debtor_id: int,
        allow_unsafe_deletion: bool = None,
        negligible_amount: float = None,
        is_scheduled_for_deletion: bool = None) -> None:

    """Change account's configuration.

    Raises `AccountDoesNotExistError` if the account does not exist.

    """

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    account = get_account(creditor_id, debtor_id)  # TODO: use lock and joinedload
    if account is None:
        raise AccountDoesNotExistError()

    config = account.config
    data = account.data
    if allow_unsafe_deletion is not None and config.allow_unsafe_deletion != allow_unsafe_deletion:
        config.allow_unsafe_deletion = allow_unsafe_deletion

    if negligible_amount is not None and config.negligible_amount != negligible_amount:
        assert negligible_amount >= 0.0
        config.negligible_amount = negligible_amount
        data.is_config_effectual = False

    if is_scheduled_for_deletion is not None and config.is_scheduled_for_deletion != is_scheduled_for_deletion:
        config.is_scheduled_for_deletion = is_scheduled_for_deletion
        data.is_config_effectual = False

    if not data.is_config_effectual:
        _insert_configure_account_signal(config, data)


@atomic
def try_to_remove_account(creditor_id: int, debtor_id: int) -> bool:
    """Try to remove an account, return if the account has been removed."""

    # TODO: Make sure users do not remove accounts unsafely too
    #       often. For example, users may create and remove hundreds
    #       of accounts per minute, significantly raising the cost for
    #       the operator of the service.

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    account = get_account(creditor_id, debtor_id)  # TODO: use joinedload.
    if account:
        config = account.config
        data = account.data
        if not config.allow_unsafe_deletion:
            days_since_last_config_signal = (datetime.now(tz=timezone.utc) - data.last_config_ts).days
            is_timed_out = days_since_last_config_signal > current_app.config['APP_DEAD_ACCOUNTS_ABANDON_DAYS']
            is_effectually_scheduled_for_deletion = config.is_scheduled_for_deletion and data.is_config_effectual
            is_removal_safe = not data.has_server_account and (is_effectually_scheduled_for_deletion or is_timed_out)
            if not is_removal_safe:
                return False

        Account.query.\
            filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
            delete(synchronize_session=False)

    return True


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
    # TODO: Do not foget to do the same thing when the account is dead
    #       (no heartbeat for a long time).

    account_data = AccountData.lock_instance((creditor_id, debtor_id))
    if account_data and account_data.creation_date == creation_date:
        # TODO: reset other fields as well?
        account_data.has_server_account = False
        account_data.principal = 0
        account_data.interest = 0.0


@atomic
def process_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_interest_rate_change_ts: datetime,
        last_transfer_number: int,
        last_transfer_committed_at_ts: datetime,
        last_config_ts: datetime,
        last_config_seqnum: int,
        creation_date: date,
        negligible_amount: float,
        config: str,
        config_flags: int,
        status_flags: int,
        ts: datetime,
        ttl: int,
        account_id: str,
        debtor_info_url: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= last_change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL
    assert 0 <= last_transfer_number <= MAX_INT64
    assert MIN_INT32 <= last_config_seqnum <= MAX_INT32
    assert negligible_amount >= 0.0
    assert MIN_INT32 <= config_flags <= MAX_INT32
    assert MIN_INT32 <= status_flags <= MAX_INT32
    assert ttl > 0

    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    query = db.session.query(AccountData, AccountConfig).\
        outerjoin(AccountConfig, ACCOUNT_CONFIG_JOIN_CLAUSE).\
        filter(AccountData.creditor_id == creditor_id, AccountData.debtor_id == debtor_id).\
        with_for_update(of=AccountData)

    try:
        account_data, account_config = query.one()
    except exc.NoResultFound:
        # TODO: Should we schedule the account for deletion here?
        return

    if creation_date < account_data.creation_date:
        return

    if ts > account_data.last_heartbeat_ts:
        account_data.last_heartbeat_ts = min(ts, current_ts)

    prev_event = (account_data.creation_date, account_data.last_change_ts, Seqnum(account_data.last_change_seqnum))
    this_event = (creation_date, last_change_ts, Seqnum(last_change_seqnum))
    is_new_event = this_event > prev_event
    if not is_new_event:
        return

    last_config_request = (account_data.last_config_ts, Seqnum(account_data.last_config_seqnum))
    applied_config_request = (last_config_ts, Seqnum(last_config_seqnum))
    is_same_config = all([
        account_config.negligible_amount == negligible_amount,
        account_config.config == config,
        account_config.config_flags == config_flags,
    ])
    is_config_effectual = last_config_request == applied_config_request and is_same_config

    account_data.has_server_account = True
    account_data.creation_date = creation_date
    account_data.last_change_ts = last_change_ts
    account_data.last_change_seqnum = last_change_seqnum
    account_data.principal = principal
    account_data.interest = interest
    account_data.interest_rate = interest_rate
    account_data.last_interest_rate_change_ts = last_interest_rate_change_ts
    account_data.last_transfer_number = last_transfer_number,
    account_data.last_transfer_committed_at_ts = last_transfer_committed_at_ts
    account_data.status_flags = status_flags
    account_data.account_id = account_id
    account_data.debtor_info_url = debtor_info_url
    account_data.is_config_effectual = is_config_effectual
    account_data.info_latest_update_id = 1  # TODO: generate it.
    account_data.info_latest_update_ts = current_ts
    account_data.ledger_latest_update_id = 1  # TODO: generate it.
    account_data.ledger_latest_update_ts = current_ts
    if is_config_effectual:
        account_data.config_error = None
    elif applied_config_request >= last_config_request:
        # Normally, this should never happen.
        account_data.config_error = 'SUPERSEDED_CONFIGURATION'

    # TODO: Consider resetting the ledger if
    #       `account_data.creation_date < creation_date`.


@atomic
def process_account_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_number: int,
        coordinator_type: str,
        committed_at_ts: datetime,
        acquired_amount: int,
        transfer_note: str,
        creation_date: date,
        principal: int,
        previous_transfer_number: int,
        sender: str,
        recipient: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert 0 < transfer_number <= MAX_INT64
    assert len(coordinator_type) <= 30
    assert acquired_amount != 0
    assert -MAX_INT64 <= acquired_amount <= MAX_INT64
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert 0 <= previous_transfer_number <= MAX_INT64
    assert previous_transfer_number < transfer_number

    try:
        account_data = AccountData.get_instance((creditor_id, debtor_id))
    except CreditorDoesNotExistError:
        return

    committed_transfer = CommittedTransfer(
        account_data=account_data,
        transfer_number=transfer_number,
        coordinator_type=coordinator_type,
        committed_at_ts=committed_at_ts,
        acquired_amount=acquired_amount,
        transfer_note=transfer_note,
        creation_date=creation_date,
        principal=principal,
        sender=sender,
        recipient=recipient,
    )
    try:
        db.session.add(committed_transfer)
        db.session.flush()
    except IntegrityError:
        # Normally, this can happen only when the account commit
        # message has been re-delivered. Therefore, no action should
        # be taken.
        db.session.rollback()
        return

    current_ts = datetime.now(tz=timezone.utc)
    if creation_date > account_data.creation_date:
        account_data.reset(account_creation_date=creation_date, current_ts=current_ts)

    ledger_has_not_been_updated_soon = current_ts - account_data.ledger_latest_update_ts > TD_5_SECONDS
    if transfer_number == account_data.ledger_next_transfer_number and ledger_has_not_been_updated_soon:
        # If account commits come in the right order, it is faster to
        # update the account ledger right away. We must be careful,
        # though, not to update the account ledger too often, because
        # this can cause a row lock contention.
        _update_ledger(account_data, principal, current_ts)
        _insert_ledger_entry(creditor_id, debtor_id, transfer_number, acquired_amount, principal)
    elif transfer_number >= account_data.ledger_next_transfer_number:
        # A dedicated asynchronous task will do the addition to the account
        # ledger later. (See `process_pending_account_commits()`.)
        db.session.add(PendingAccountCommit(
            committed_transfer=committed_transfer,
            account_new_principal=principal,
            committed_at_ts=committed_at_ts,
            committed_amount=acquired_amount,
        ))


@atomic
def process_pending_account_commits(creditor_id: int, debtor_id: int, max_count: int = None) -> bool:
    """Return `False` if some legible account commits remained unprocessed."""

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    ledger = _get_ledger(creditor_id, debtor_id, lock=True)
    if ledger is None:
        return True

    has_gaps = False
    pks_to_delete = []
    current_ts = datetime.now(tz=timezone.utc)
    pending_transfers = _get_ordered_pending_transfers(ledger, max_count)
    for transfer_number, committed_amount, account_new_principal in pending_transfers:
        pk = (creditor_id, debtor_id, transfer_number)
        if transfer_number == ledger.next_transfer_number:
            _update_ledger(ledger, account_new_principal, current_ts)
            _insert_ledger_entry(*pk, committed_amount, account_new_principal)
        elif transfer_number > ledger.next_transfer_number:
            has_gaps = True
            break
        pks_to_delete.append(pk)

    PendingAccountCommit.query.\
        filter(PENDING_ACCOUNT_COMMIT_PK.in_(pks_to_delete)).\
        delete(synchronize_session=False)
    return has_gaps or max_count is None or len(pending_transfers) < max_count


@atomic
def find_legible_pending_account_commits(max_count: int = None):
    pac = PendingAccountCommit
    al = 'AccountLedger'
    query = db.session.query(pac.creditor_id, pac.debtor_id).filter(
        al.creditor_id == pac.creditor_id,
        al.debtor_id == pac.debtor_id,
        al.next_transfer_number == pac.transfer_number
    )
    if max_count is not None:
        query = query.limit(max_count)
    return query.all()


@atomic
def process_finalized_direct_transfer_signal(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        recipient: str,
        committed_amount: int,
        status_code: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert 0 <= committed_amount <= MAX_INT64
    assert 0 <= len(status_code.encode('ascii')) <= 30

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == sender_creditor_id
        and rt.direct_transfer_id == transfer_id
    )
    if rt_matches_the_signal:
        assert rt is not None
        if committed_amount == rt.amount and recipient == rt.recipient:
            error = None
        elif committed_amount == 0 and recipient == rt.recipient:
            error = {'errorCode': status_code}
        else:
            error = {'errorCode': 'UNEXPECTED_ERROR'}
        _finalize_direct_transfer(rt.debtor_id, rt.transfer_uuid, error=error)
        db.session.delete(rt)


@atomic
def delete_direct_transfer(debtor_id: int, transfer_uuid: UUID) -> bool:
    number_of_deleted_rows = DirectTransfer.query.\
        filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).\
        delete(synchronize_session=False)

    assert number_of_deleted_rows in [0, 1]
    if number_of_deleted_rows == 1:
        # TODO: Update creditor's direct transfers count.

        # Note that deleting the `RunningTransfer` record may result
        # in dismissing an already committed transfer. This is not a
        # problem in this case, however, because the user has ordered
        # the deletion of the `DirectTransfer` record, and therefore
        # is not interested in the its outcome.
        RunningTransfer.query.\
            filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).\
            delete(synchronize_session=False)

    return number_of_deleted_rows == 1


def _add_log_entry(
        creditor: Creditor,
        *,
        object_type: str,
        object_uri: str,
        is_deleted: bool = False,
        data: dict = None,
        current_ts: datetime = None) -> Tuple[int, datetime]:

    current_ts = current_ts or datetime.now(tz=timezone.utc)
    creditor_id = creditor.creditor_id
    previous_entry_id = creditor.latest_log_entry_id
    entry_id = creditor.generate_log_entry_id()
    db.session.add(LogEntry(
        creditor_id=creditor_id,
        entry_id=entry_id,
        previous_entry_id=previous_entry_id,
        object_type=object_type,
        object_uri=object_uri,
        added_at_ts=current_ts,
        is_deleted=is_deleted,
        data=data,
    ))

    return entry_id, current_ts


def _insert_configure_account_signal(config: AccountConfig, data: AccountData, current_ts: datetime = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    data.is_config_effectual = False
    data.last_config_ts = max(data.last_config_ts, current_ts)
    data.last_config_seqnum = increment_seqnum(data.last_config_seqnum)
    db.session.add(ConfigureAccountSignal(
        debtor_id=data.debtor_id,
        creditor_id=data.creditor_id,
        ts=data.last_config_ts,
        seqnum=data.last_config_seqnum,
        negligible_amount=config.negligible_amount,
        config_flags=config.config_flags,
        config=config.config,
    ))


def _discard_orphaned_account(
        creditor_id: int,
        debtor_id: int,
        status_flags: int,
        config_flags: int,
        negligible_amount: float) -> None:

    is_scheduled_for_deletion = config_flags & AccountConfig.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    has_huge_negligible_amount = negligible_amount >= HUGE_NEGLIGIBLE_AMOUNT
    if not (is_scheduled_for_deletion and has_huge_negligible_amount):
        db.session.add(ConfigureAccountSignal(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            ts=datetime.now(tz=timezone.utc),
            seqnum=0,
            negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
            is_scheduled_for_deletion=True,
        ))


def _insert_ledger_entry(
        creditor_id: int,
        debtor_id: int,
        transfer_number: int,
        committed_amount: int,
        account_new_principal: int) -> None:

    db.session.add(LedgerEntry(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_number=transfer_number,
        committed_amount=committed_amount,
        account_new_principal=account_new_principal,
    ))


def _create_account(creditor_id: int, debtor_id: int, current_ts: datetime = None) -> Account:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    creditor = Creditor.lock_instance(creditor_id)
    if creditor is None:
        raise CreditorDoesNotExistError()

    log_entry_id = creditor.generate_log_entry_id()
    latest_update = {'latest_update_id': log_entry_id, 'latest_update_ts': current_ts}
    account = Account(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        created_at_ts=current_ts,
        knowledge=AccountKnowledge(**latest_update),
        exchange=AccountExchange(**latest_update),
        display=AccountDisplay(**latest_update),
        config=AccountConfig(**latest_update),
        data=AccountData(
            info_latest_update_id=log_entry_id,
            info_latest_update_ts=current_ts,
            ledger_latest_update_id=log_entry_id,
            ledger_latest_update_ts=current_ts,
        ),
        **latest_update,
    )
    with db.retry_on_integrity_error():
        db.session.add(account)

    return account


def _get_ordered_pending_transfers(account_data: AccountData, max_count: int = None) -> List[Tuple[int, int]]:
    creditor_id = account_data.creditor_id
    debtor_id = account_data.debtor_id
    query = db.session.\
        query(
            PendingAccountCommit.transfer_number,
            PendingAccountCommit.committed_amount,
            PendingAccountCommit.account_new_principal,
        ).\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        order_by(PendingAccountCommit.transfer_number)
    if max_count is not None:
        query = query.limit(max_count)
    return query.all()


def _find_running_transfer(coordinator_id: int, coordinator_request_id: int) -> Optional[RunningTransfer]:
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 < coordinator_request_id <= MAX_INT64

    return RunningTransfer.query.\
        filter_by(creditor_id=coordinator_id, direct_coordinator_request_id=coordinator_request_id).\
        with_for_update().\
        one_or_none()


def _finalize_direct_transfer(
        debtor_id: int,
        transfer_uuid: int,
        finalized_at_ts: datetime = None,
        error: dict = None) -> None:

    direct_transfer = DirectTransfer.lock_instance((debtor_id, transfer_uuid))
    if direct_transfer and direct_transfer.finalized_at_ts is None:
        direct_transfer.finalized_at_ts = finalized_at_ts or datetime.now(tz=timezone.utc)
        direct_transfer.is_successful = error is None
        if error is not None:
            direct_transfer.error = error
