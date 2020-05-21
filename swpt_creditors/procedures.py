from uuid import UUID
from datetime import datetime, date, timedelta, timezone
from typing import TypeVar, Callable, Tuple, List, Optional
from flask import current_app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import tuple_
from sqlalchemy.orm import joinedload
from swpt_lib.utils import Seqnum, increment_seqnum
from .extensions import db
from .models import Creditor, AccountLedger, LedgerEntry, AccountCommit,  \
    Account, AccountConfig, ConfigureAccountSignal, PendingAccountCommit, \
    DirectTransfer, RunningTransfer, \
    MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, BEGINNING_OF_TIME

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_SECOND = timedelta(seconds=1)
TD_5_SECONDS = timedelta(seconds=5)
HUGE_NEGLIGIBLE_AMOUNT = 1e30
PENDING_ACCOUNT_COMMIT_PK = tuple_(
    PendingAccountCommit.debtor_id,
    PendingAccountCommit.creditor_id,
    PendingAccountCommit.transfer_seqnum,
)


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

    def __init__(self, account_config: AccountConfig):
        self.account_config = account_config


class AccountsConflictError(Exception):
    """A different account with the same debtor ID already exists."""


@atomic
def get_creditor(creditor_id: int) -> Optional[Creditor]:
    return Creditor.get_instance(creditor_id)


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
def lock_or_create_creditor(creditor_id: int) -> Creditor:
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    creditor = Creditor.lock_instance(creditor_id)
    if creditor is None:
        creditor = Creditor(creditor_id=creditor_id)
        with db.retry_on_integrity_error():
            db.session.add(creditor)
    return creditor


@atomic
def create_account(creditor_id: int, debtor_id: int) -> bool:
    """"Make sure the account exists, return if a new account was created.

    Raises `CreditorDoesNotExistError` if the creditor does not exist.

    """

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    config_needs_to_be_created = _get_account_config(creditor_id, debtor_id) is None
    if config_needs_to_be_created:
        config = _create_account_config(creditor_id, debtor_id)
        _insert_configure_account_signal(config)

    return config_needs_to_be_created


@atomic
def change_account_config(
        creditor_id: int,
        debtor_id: int,
        allow_unsafe_removal: bool = None,
        negligible_amount: float = None,
        is_scheduled_for_deletion: bool = None) -> None:

    """Change account's configuration.

    Raises `AccountDoesNotExistError` if the account does not exist.

    """

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    config = _get_account_config(creditor_id, debtor_id, lock=True)
    if config is None:
        raise AccountDoesNotExistError()

    if allow_unsafe_removal is not None and config.allow_unsafe_removal != allow_unsafe_removal:
        config.allow_unsafe_removal = allow_unsafe_removal

    if negligible_amount is not None and config.negligible_amount != negligible_amount:
        assert negligible_amount >= 0.0
        config.negligible_amount = negligible_amount
        config.is_effectual = False

    if is_scheduled_for_deletion is not None and config.is_scheduled_for_deletion != is_scheduled_for_deletion:
        config.is_scheduled_for_deletion = is_scheduled_for_deletion
        config.is_effectual = False

    if not config.is_effectual:
        _insert_configure_account_signal(config)


@atomic
def try_to_remove_account(creditor_id: int, debtor_id: int) -> bool:
    """Try to remove an account, return if the account has been removed."""

    # TODO: Make sure users do not remove accounts unsafely too
    #       often. For example, users may create and remove hundreds
    #       of accounts per minute, significantly raising the cost for
    #       the operator of the service.

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    config = _get_account_config(creditor_id, debtor_id)
    if config:
        if not config.allow_unsafe_removal:
            days_since_last_config_signal = (datetime.now(tz=timezone.utc) - config.last_ts).days
            is_timed_out = days_since_last_config_signal > current_app.config['APP_DEAD_ACCOUNTS_ABANDON_DAYS']
            is_effectually_scheduled_for_deletion = config.is_scheduled_for_deletion and config.is_effectual
            is_removal_safe = not config.has_account and (is_effectually_scheduled_for_deletion or is_timed_out)
            if not is_removal_safe:
                return False

        AccountConfig.query.\
            filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
            delete(synchronize_session=False)
        AccountLedger.query.\
            filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
            delete(synchronize_session=False)

    return True


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
    # TODO: Do not foget to do the same thing when the account is dead
    #       (no heartbeat for a long time).

    account = Account.lock_instance(
        (creditor_id, debtor_id),
        joinedload('account_config', innerjoin=True),
    )
    if account and account.creation_date == creation_date:
        config = account.account_config
        config.has_account = False
        db.session.delete(account)


@atomic
def process_account_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_ts: datetime,
        change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_transfer_seqnum: int,
        last_config_ts: datetime,
        last_config_seqnum: int,
        creation_date: date,
        negligible_amount: float,
        status: int,
        ts: datetime,
        ttl: float,
        account_identity: str,
        config: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT32 <= change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL
    assert 0 <= last_transfer_seqnum <= MAX_INT64
    assert MIN_INT32 <= last_config_seqnum <= MAX_INT32
    assert negligible_amount >= 0.0
    assert MIN_INT32 <= status <= MAX_INT32
    assert ttl > 0.0

    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    account = Account.lock_instance(
        (creditor_id, debtor_id),
        joinedload('account_config', innerjoin=True),
        of=Account,
    )
    if account:
        if account.creation_date > creation_date:  # pragma: no cover
            # This should never happen, given that the `swpt_accounts`
            # service behaves adequately. Nevertheless, it is good to
            # be prepared for all eventualities.
            return
        prev_event = (account.creation_date, account.change_ts, Seqnum(account.change_seqnum))
        this_event = (creation_date, change_ts, Seqnum(change_seqnum))
        if this_event >= prev_event:
            account.last_heartbeat_ts = ts
        if this_event <= prev_event:
            return
        new_account = account.creation_date < creation_date
        account.change_ts = change_ts
        account.change_seqnum = change_seqnum
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_transfer_seqnum = last_transfer_seqnum
        account.creation_date = creation_date
        account.negligible_amount = negligible_amount
        account.status = status
    else:
        config = _get_account_config(creditor_id, debtor_id, lock=True)
        if config is None:
            # TODO: This is very dangerous. Ensure that `creditor_id`
            #       matches the CREDITORSPACE/CREDITORSPACE_MASK.

            # The user have removed the account. The "orphaned"
            # `Account` record should be scheduled for deletion.
            _discard_orphaned_account(creditor_id, debtor_id, status, negligible_amount)
            return
        new_account = True
        account = Account(
            account_config=config,
            change_ts=change_ts,
            change_seqnum=change_seqnum,
            principal=principal,
            interest=interest,
            interest_rate=interest_rate,
            last_transfer_seqnum=last_transfer_seqnum,
            creation_date=creation_date,
            negligible_amount=negligible_amount,
            status=status,
        )
        with db.retry_on_integrity_error():
            db.session.add(account)

    _revise_account_config_effectuality(
        account,
        last_config_ts,
        last_config_seqnum,
        new_account,
        account_identity,
        config,
    )

    # TODO: Reset the ledger if it has been outdated for a long time.
    #       Consider adding `Account.last_transfer_committed_at_ts`
    #       and `Account.last_transfer_ts`.

    # TODO: Detect a change in the interest rate. Make sure there is
    #       an `AccountIssue` record informing for the event.


@atomic
def process_account_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_seqnum: int,
        coordinator_type: str,
        committed_at_ts: datetime,
        committed_amount: int,
        other_party_identity: str,
        transfer_message: str,
        transfer_flags: int,
        account_creation_date: date,
        account_new_principal: int,
        previous_transfer_seqnum: int,
        system_flags: int,
        creditor_identity: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert 0 < transfer_seqnum <= MAX_INT64
    assert len(coordinator_type) <= 30
    assert committed_amount != 0
    assert -MAX_INT64 <= committed_amount <= MAX_INT64
    assert MIN_INT32 <= transfer_flags <= MAX_INT32
    assert -MAX_INT64 <= account_new_principal <= MAX_INT64
    assert 0 <= previous_transfer_seqnum <= MAX_INT64
    assert previous_transfer_seqnum < transfer_seqnum
    assert MIN_INT32 <= system_flags <= MAX_INT32

    try:
        ledger = _get_or_create_ledger(creditor_id, debtor_id)
    except CreditorDoesNotExistError:
        return

    account_commit = AccountCommit(
        account_ledger=ledger,
        transfer_seqnum=transfer_seqnum,
        coordinator_type=coordinator_type,
        other_party_identity=other_party_identity,
        committed_at_ts=committed_at_ts,
        committed_amount=committed_amount,
        transfer_message=transfer_message,
        transfer_flags=transfer_flags,
        account_creation_date=account_creation_date,
        account_new_principal=account_new_principal,
        system_flags=system_flags,
        creditor_identity=creditor_identity,
    )
    try:
        db.session.add(account_commit)
        db.session.flush()
    except IntegrityError:
        # Normally, this can happen only when the account commit
        # message has been re-delivered. Therefore, no action should
        # be taken.
        db.session.rollback()
        return

    current_ts = datetime.now(tz=timezone.utc)
    if account_creation_date > ledger.account_creation_date:
        ledger.reset(account_creation_date=account_creation_date, current_ts=current_ts)

    ledger_has_not_been_updated_soon = current_ts - ledger.last_update_ts > TD_5_SECONDS
    if transfer_seqnum == ledger.next_transfer_seqnum and ledger_has_not_been_updated_soon:
        # If account commits come in the right order, it is faster to
        # update the account ledger right away. We must be careful,
        # though, not to update the account ledger too often, because
        # this can cause a row lock contention.
        _update_ledger(ledger, account_new_principal, current_ts)
        _insert_ledger_entry(creditor_id, debtor_id, transfer_seqnum, committed_amount, account_new_principal)
    elif transfer_seqnum >= ledger.next_transfer_seqnum:
        # A dedicated asynchronous task will do the addition to the account
        # ledger later. (See `process_pending_account_commits()`.)
        db.session.add(PendingAccountCommit(
            account_commit=account_commit,
            account_new_principal=account_new_principal,
            committed_at_ts=committed_at_ts,
            committed_amount=committed_amount,
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
    for transfer_seqnum, committed_amount, account_new_principal in pending_transfers:
        pk = (creditor_id, debtor_id, transfer_seqnum)
        if transfer_seqnum == ledger.next_transfer_seqnum:
            _update_ledger(ledger, account_new_principal, current_ts)
            _insert_ledger_entry(*pk, committed_amount, account_new_principal)
        elif transfer_seqnum > ledger.next_transfer_seqnum:
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
    al = AccountLedger
    query = db.session.query(pac.creditor_id, pac.debtor_id).filter(
        al.creditor_id == pac.creditor_id,
        al.debtor_id == pac.debtor_id,
        al.next_transfer_seqnum == pac.transfer_seqnum
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


def _insert_configure_account_signal(config: AccountConfig, current_ts: datetime = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    config.is_effectual = False
    config.last_ts = max(config.last_ts, current_ts)
    config.last_seqnum = increment_seqnum(config.last_seqnum)
    db.session.add(ConfigureAccountSignal(
        creditor_id=config.creditor_id,
        debtor_id=config.debtor_id,
        ts=config.last_ts,
        seqnum=config.last_seqnum,
        negligible_amount=config.negligible_amount,
        is_scheduled_for_deletion=config.is_scheduled_for_deletion,
    ))


def _discard_orphaned_account(creditor_id: int, debtor_id: int, status: int, negligible_amount: float) -> None:
    is_deleted = status & Account.STATUS_DELETED_FLAG
    is_scheduled_for_deletion = status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    has_huge_negligible_amount = negligible_amount >= HUGE_NEGLIGIBLE_AMOUNT
    if not is_deleted and not (is_scheduled_for_deletion and has_huge_negligible_amount):
        db.session.add(ConfigureAccountSignal(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            ts=datetime.now(tz=timezone.utc),
            seqnum=0,
            negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
            is_scheduled_for_deletion=True,
        ))


def _get_ledger(creditor_id: int, debtor_id: int, lock: bool = False) -> Optional[AccountLedger]:
    if lock:
        ledger = AccountLedger.lock_instance((creditor_id, debtor_id))
    else:
        ledger = AccountLedger.get_instance((creditor_id, debtor_id))
    return ledger


def _create_ledger(creditor_id: int, debtor_id: int) -> AccountLedger:
    """Insert an `AccountLedger` row with a corresponding `AccountConfig` row."""

    if Creditor.get_instance(creditor_id) is None:
        raise CreditorDoesNotExistError()

    ledger = AccountLedger(creditor_id=creditor_id, debtor_id=debtor_id, account_config=AccountConfig())
    with db.retry_on_integrity_error():
        db.session.add(ledger)
    return ledger


def _get_or_create_ledger(creditor_id: int, debtor_id: int, lock: bool = False) -> AccountLedger:
    return _get_ledger(creditor_id, debtor_id, lock=lock) or _create_ledger(creditor_id, debtor_id)


def _update_ledger(ledger: AccountLedger, account_new_principal: int, current_ts: datetime = None) -> None:
    ledger.principal = account_new_principal
    ledger.next_transfer_seqnum += 1
    ledger.last_update_ts = current_ts or datetime.now(tz=timezone.utc)


def _insert_ledger_entry(
        creditor_id: int,
        debtor_id: int,
        transfer_seqnum: int,
        committed_amount: int,
        account_new_principal: int) -> None:

    db.session.add(LedgerEntry(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_seqnum=transfer_seqnum,
        committed_amount=committed_amount,
        account_new_principal=account_new_principal,
    ))


def _create_account_config(creditor_id: int, debtor_id: int) -> AccountConfig:
    ledger = _get_ledger(creditor_id, debtor_id)

    # When this function is called, it is almost 100% certain that
    # there will be no corresponding ledger record. Nevertheless, it
    # is good to be prepared for this eventuality.
    if ledger is None:
        config = _create_ledger(creditor_id, debtor_id).account_config
    else:
        config = AccountConfig(account_ledger=ledger)
        with db.retry_on_integrity_error():
            db.session.add(config)

    assert config
    return config


def _get_account_config(creditor_id: int, debtor_id: int, lock: bool = False) -> Optional[AccountConfig]:
    if lock:
        config = AccountConfig.lock_instance((creditor_id, debtor_id))
    else:
        config = AccountConfig.get_instance((creditor_id, debtor_id))
    return config


def _get_or_create_account_config(creditor_id: int, debtor_id: int, lock: bool = False) -> AccountConfig:
    return _get_account_config(creditor_id, debtor_id, lock=lock) or _create_account_config(creditor_id, debtor_id)


def _revise_account_config_effectuality(
        account: Account,
        last_config_ts: datetime,
        last_config_seqnum: int,
        new_account: bool,
        account_identity: str,
        config: str) -> None:

    config = account.account_config

    if not config.has_account:
        config.has_account = True

    if config.account_identity is None:
        config.account_identity = account_identity

    no_applied_config = last_config_ts - BEGINNING_OF_TIME < TD_SECOND
    if no_applied_config:
        # It looks like the account has been resurrected with the
        # default configuration values, and must be reconfigured. As
        # an optimization, we do this reconfiguration only once (when
        # the account is new).
        if new_account:
            _insert_configure_account_signal(config)
    else:
        last_config_request = (config.last_ts, Seqnum(config.last_seqnum))
        last_applied_config = (last_config_ts, Seqnum(last_config_seqnum))
        config_is_effectual = account.check_if_config_is_effectual() and account_identity == config.account_identity
        if last_applied_config >= last_config_request and config.is_effectual != config_is_effectual:
            config.is_effectual = config_is_effectual

    # TODO: Verify the effectuallity of the `config.config` field.

    # TODO: Detect the situation when the account is scheduled for
    #       deletion, but `config.negligible_amount` is smaller than
    #       available amount. Make sure there is an `AccountIssue`
    #       record informing for the event.


def _get_ordered_pending_transfers(ledger: AccountLedger, max_count: int = None) -> List[Tuple[int, int]]:
    creditor_id = ledger.creditor_id
    debtor_id = ledger.debtor_id
    query = db.session.\
        query(
            PendingAccountCommit.transfer_seqnum,
            PendingAccountCommit.committed_amount,
            PendingAccountCommit.account_new_principal,
        ).\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        order_by(PendingAccountCommit.transfer_seqnum)
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
