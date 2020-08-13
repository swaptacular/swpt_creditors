from uuid import UUID
from datetime import datetime, date, timedelta, timezone
from typing import TypeVar, Callable, Tuple, List, Optional, Iterable
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import tuple_, null, true
from sqlalchemy.orm import joinedload, exc, load_only, Load
from swpt_lib.utils import Seqnum, increment_seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    Creditor, LedgerEntry, CommittedTransfer, Account, AccountData, PendingLogEntry,
    ConfigureAccountSignal, PendingAccountCommit, LogEntry, AccountDisplay,
    AccountExchange, AccountKnowledge, DirectTransfer, RunningTransfer,
    MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL,
    DEFAULT_CONFIG_FLAGS, DEFAULT_NEGLIGIBLE_AMOUNT,
)

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_SECOND = timedelta(seconds=1)
TD_5_SECONDS = timedelta(seconds=5)
PENDING_ACCOUNT_COMMIT_PK = tuple_(
    PendingAccountCommit.debtor_id,
    PendingAccountCommit.creditor_id,
    PendingAccountCommit.transfer_number,
)
ACCOUNT_DATA_CONFIG_RELATED_COLUMNS = [
    'creditor_id',
    'debtor_id',
    'creation_date',
    'last_config_ts',
    'last_config_seqnum',
    'negligible_amount',
    'config_flags',
    'allow_unsafe_deletion',
    'is_config_effectual',
    'config_error',
    'config_latest_update_id',
    'config_latest_update_ts',
    'has_server_account',
    'info_latest_update_id',
    'info_latest_update_ts',
]
ACCOUNT_DATA_LEDGER_RELATED_COLUMNS = [
    'creditor_id',
    'debtor_id',
    'creation_date',
    'ledger_principal',
    'ledger_latest_entry_id',
    'ledger_latest_update_id',
    'ledger_latest_update_ts',
    'ledger_last_transfer_number',
    'ledger_last_transfer_committed_at_ts',
    'principal',
    'interest',
    'interest_rate',
    'last_change_ts',
]
ACCOUNT_DATA_INFO_RELATED_COLUMNS = [
    'creditor_id',
    'debtor_id',
    'creation_date',
    'account_id',
    'status_flags',
    'config_flags',
    'config_error',
    'is_config_effectual',
    'has_server_account',
    'interest_rate',
    'last_interest_rate_change_ts',
    'debtor_info_url',
    'principal',
    'interest',
    'info_latest_update_id',
    'info_latest_update_ts',
]


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


class AccountsConflictError(Exception):
    """A different account with the same debtor ID already exists."""


class UnsafeAccountDeletionError(Exception):
    """Unauthorized unsafe deletion of an account."""


class PegAccountDeletionError(Exception):
    """Can not delete an account that acts as a currency peg."""


class InvalidExchangePolicyError(Exception):
    """Invalid exchange policy."""


class AccountDebtorNameConflictError(Exception):
    """Another account with this debtorName already exist."""


class AccountOwnUnitConflictError(Exception):
    """Another another account with this ownUnit already exist."""


@atomic
def get_creditors_with_pending_log_entries() -> Iterable[int]:
    return set(t[0] for t in db.session.query(PendingLogEntry.creditor_id).all())


@atomic
def process_pending_log_entries(creditor_id: int) -> None:
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    pending_log_entries = PendingLogEntry.query.\
        filter_by(creditor_id=creditor_id).\
        order_by(PendingLogEntry.pending_entry_id).\
        with_for_update().\
        all()

    if pending_log_entries:
        creditor = Creditor.lock_instance(creditor_id)

        for entry in pending_log_entries:
            previous_entry_id = creditor.latest_log_entry_id
            entry_id = creditor.generate_log_entry_id()
            db.session.add(LogEntry(
                creditor_id=creditor_id,
                entry_id=entry_id,
                previous_entry_id=previous_entry_id,
                object_type=entry.object_type,
                object_uri=entry.object_uri,
                object_update_id=entry.object_update_id,
                added_at_ts=entry.added_at_ts,
                is_deleted=entry.is_deleted,
                data=entry.data,
            ))
            db.session.delete(entry)


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
def get_creditor(creditor_id: int, lock: bool = False) -> Optional[Creditor]:
    if lock:
        creditor = Creditor.lock_instance(creditor_id)
    else:
        creditor = Creditor.get_instance(creditor_id)

    if creditor is None or creditor.deactivated_at_date is not None:
        return None

    return creditor


@atomic
def update_creditor(creditor_id: int) -> Creditor:
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise CreditorDoesNotExistError()

    creditor.creditor_latest_update_id += 1
    creditor.creditor_latest_update_ts = current_ts
    _add_log_entry(
        creditor,
        object_type=types.creditor,
        object_uri=paths.creditor(creditorId=creditor_id),
        object_update_id=creditor.creditor_latest_update_id,
        current_ts=current_ts,
    )

    return creditor


@atomic
def get_creditor_log_entries(creditor_id: int, *, count: int = 1, prev: int = 0) -> Tuple[List[LogEntry], int]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert count >= 1
    assert 0 <= prev <= MAX_INT64

    latest_log_entry_id = db.session.\
        query(Creditor.latest_log_entry_id).\
        filter(Creditor.creditor_id == creditor_id, Creditor.deactivated_at_date == null()).\
        scalar()

    if latest_log_entry_id is None:
        raise CreditorDoesNotExistError()

    log_entries = LogEntry.query.\
        filter(LogEntry.creditor_id == creditor_id).\
        filter(LogEntry.entry_id > prev).\
        order_by(LogEntry.entry_id).\
        limit(count).\
        all()

    return log_entries, latest_log_entry_id


@atomic
def get_creditor_debtor_ids(creditor_id: int, count: int = 1, prev: int = None) -> List[int]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert count >= 1
    assert prev is None or MIN_INT64 <= prev <= MAX_INT64

    query = db.session.\
        query(Account.debtor_id).\
        filter(Account.creditor_id == creditor_id).\
        order_by(Account.debtor_id)

    if prev is not None:
        query = query.filter(Account.debtor_id > prev)

    return [t[0] for t in query.limit(count).all()]


@atomic
def has_account(creditor_id: int, debtor_id: Optional[int]) -> bool:
    if debtor_id is None:
        return False

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    account_query = Account.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    return db.session.query(account_query.exists()).scalar()


@atomic
def get_account(creditor_id: int, debtor_id: int) -> Optional[Account]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    options = [
        joinedload(Account.knowledge, innerjoin=True),
        joinedload(Account.exchange, innerjoin=True),
        joinedload(Account.display, innerjoin=True),
        joinedload(Account.data, innerjoin=True),
    ]
    return Account.get_instance((creditor_id, debtor_id), *options)


@atomic
def create_new_account(creditor_id: int, debtor_id: int) -> Account:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_creditor(creditor_id, lock=True)

    if creditor is None:
        raise CreditorDoesNotExistError()

    if has_account(creditor_id, debtor_id):
        raise AccountExistsError()

    return _create_new_account(creditor, debtor_id, current_ts)


@atomic
def get_account_info(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(load_only(*ACCOUNT_DATA_INFO_RELATED_COLUMNS)).\
        one_or_none()


@atomic
def get_account_ledger(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(load_only(*ACCOUNT_DATA_LEDGER_RELATED_COLUMNS)).\
        one_or_none()


@atomic
def get_account_config(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)).\
        one_or_none()


@atomic
def update_account_config(
        creditor_id: int,
        debtor_id: int,
        is_scheduled_for_deletion: bool,
        negligible_amount: float,
        allow_unsafe_deletion: bool) -> AccountData:

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    options = [load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)]
    data = AccountData.lock_instance((creditor_id, debtor_id), *options)
    if data is None:
        raise AccountDoesNotExistError()

    # NOTE: The account will not be safe to delete once the config is
    # updated. Therefore, if the account is safe to delete now, we
    # need to inform the client about the upcoming change.
    if data.is_deletion_safe:
        data.info_latest_update_id += 1
        data.info_latest_update_ts = current_ts
        db.session.add(PendingLogEntry(
            creditor_id=creditor_id,
            added_at_ts=current_ts,
            object_type=types.account_info,
            object_uri=paths.account_info(creditorId=creditor_id, debtorId=debtor_id),
            object_update_id=data.info_latest_update_id,
        ))

    data.last_config_ts = current_ts
    data.last_config_seqnum = increment_seqnum(data.last_config_seqnum)
    data.is_config_effectual = False
    data.is_scheduled_for_deletion = is_scheduled_for_deletion
    data.negligible_amount = negligible_amount
    data.allow_unsafe_deletion = allow_unsafe_deletion
    data.config_latest_update_id += 1
    data.config_latest_update_ts = current_ts

    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_config,
        object_uri=paths.account_config(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=data.config_latest_update_id,
    ))
    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=data.last_config_ts,
        seqnum=data.last_config_seqnum,
        negligible_amount=data.negligible_amount,
        config_flags=data.config_flags,
        config='',
    ))

    return data


@atomic
def get_account_display(creditor_id: int, debtor_id: int) -> Optional[AccountDisplay]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    return AccountDisplay.get_instance((creditor_id, debtor_id))


@atomic
def update_account_display(
        creditor_id: int,
        debtor_id: int,
        debtor_name: Optional[str],
        amount_divisor: float,
        decimal_places: int,
        own_unit: Optional[str],
        own_unit_preference: int,
        hide: bool,
        peg_exchange_rate: Optional[float],
        peg_currency_debtor_id: Optional[int],
        peg_debtor_home_url: Optional[str]) -> AccountDisplay:

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    display = AccountDisplay.lock_instance((creditor_id, debtor_id))
    if display is None:
        raise AccountDoesNotExistError()

    # NOTE: We must ensure that the debtor name is unique.
    if debtor_name not in [display.debtor_name, None]:
        debtor_name_query = AccountDisplay.query.filter_by(creditor_id=creditor_id, debtor_name=debtor_name)
        debtor_name_confilict = db.session.query(debtor_name_query.exists()).scalar()
        if debtor_name_confilict:
            raise AccountDebtorNameConflictError()

    # NOTE: We must ensure that the own unit is unique.
    if own_unit not in [display.own_unit, None]:
        own_unit_query = AccountDisplay.query.filter_by(creditor_id=creditor_id, own_unit=own_unit)
        own_unit_conflict = db.session.query(own_unit_query.exists()).scalar()
        if own_unit_conflict:
            raise AccountOwnUnitConflictError()

    # NOTE: When a currency peg is specified, and the creditor already
    # has an account in the specified peg currency, then we must set a
    # reference to it.
    peg_account_debtor_id = peg_currency_debtor_id if has_account(creditor_id, peg_currency_debtor_id) else None

    display.debtor_name = debtor_name
    display.amount_divisor = amount_divisor
    display.decimal_places = decimal_places
    display.own_unit = own_unit
    display.own_unit_preference = own_unit_preference
    display.hide = hide
    display.peg_exchange_rate = peg_exchange_rate
    display.peg_currency_debtor_id = peg_currency_debtor_id
    display.peg_account_debtor_id = peg_account_debtor_id
    display.peg_debtor_home_url = peg_debtor_home_url
    display.latest_update_id += 1
    display.latest_update_ts = current_ts

    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_display,
        object_uri=paths.account_display(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=display.latest_update_id,
    ))

    return display


@atomic
def get_account_knowledge(creditor_id: int, debtor_id: int) -> Optional[AccountKnowledge]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    return AccountKnowledge.get_instance((creditor_id, debtor_id))


@atomic
def update_account_knowledge(
        creditor_id: int,
        debtor_id: int,
        interest_rate: float,
        interest_rate_changed_at_ts: datetime,
        account_identity: Optional[str],
        debtor_info_sha256: Optional[bytes]) -> AccountKnowledge:

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    knowledge = AccountKnowledge.lock_instance((creditor_id, debtor_id))
    if knowledge is None:
        raise AccountDoesNotExistError()

    knowledge.interest_rate = interest_rate
    knowledge.interest_rate_changed_at_ts = interest_rate_changed_at_ts
    knowledge.account_identity = account_identity
    knowledge.debtor_info_sha256 = debtor_info_sha256
    knowledge.latest_update_id += 1
    knowledge.latest_update_ts = current_ts

    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_knowledge,
        object_uri=paths.account_knowledge(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=knowledge.latest_update_id,
    ))

    return knowledge


@atomic
def get_account_exchange(creditor_id: int, debtor_id: int) -> Optional[AccountExchange]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    return AccountExchange.get_instance((creditor_id, debtor_id))


@atomic
def update_account_exchange(
        creditor_id: int,
        debtor_id: int,
        policy: Optional[str],
        min_principal: int,
        max_principal: int) -> AccountKnowledge:

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)

    # NOTE: There are no defined valid policy names yet.
    if policy is not None:
        raise InvalidExchangePolicyError()

    exchange = AccountExchange.lock_instance((creditor_id, debtor_id))
    if exchange is None:
        raise AccountDoesNotExistError()

    exchange.policy = policy
    exchange.min_principal = min_principal
    exchange.max_principal = max_principal
    exchange.latest_update_id += 1
    exchange.latest_update_ts = current_ts

    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_exchange,
        object_uri=paths.account_exchange(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=exchange.latest_update_id,
    ))

    return exchange


@atomic
def delete_account(creditor_id: int, debtor_id: int) -> None:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    query = db.session.\
        query(AccountData, Creditor).\
        join(Creditor, Creditor.creditor_id == AccountData.creditor_id).\
        filter(AccountData.creditor_id == creditor_id, AccountData.debtor_id == debtor_id).\
        with_for_update(of=Creditor).\
        options(Load(AccountData).load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS))

    try:
        data, creditor = query.one()
    except exc.NoResultFound:
        raise AccountDoesNotExistError()

    if not (data.is_deletion_safe or data.allow_unsafe_deletion):
        raise UnsafeAccountDeletionError()

    pegged_accounts_query = AccountDisplay.query.filter_by(creditor_id=creditor_id, peg_account_debtor_id=debtor_id)
    if db.session.query(pegged_accounts_query.exists()).scalar():
        raise PegAccountDeletionError()

    # NOTE: When the account gets deleted, all its related objects
    # will be deleted too. Also, the deleted account will disappear
    # from the list of accounts. Therefore, we need to write a bunch
    # of events to the log, so as to inform the client.
    creditor.account_list_latest_update_id += 1
    creditor.account_list_latest_update_ts = current_ts
    _add_log_entry(
        creditor,
        object_type=types.account_list,
        object_uri=paths.account_list(creditorId=creditor_id),
        object_update_id=creditor.account_list_latest_update_id,
        current_ts=current_ts,
    )
    deletion_events = [
        (types.account, paths.account(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_config, paths.account_config(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_info, paths.account_info(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_ledger, paths.account_ledger(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_display, paths.account_display(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_exchange, paths.account_exchange(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_knowledge, paths.account_knowledge(creditorId=creditor_id, debtorId=debtor_id)),
    ]
    for object_type, object_uri in deletion_events:
        _add_log_entry(
            creditor,
            object_type=object_type,
            object_uri=object_uri,
            is_deleted=True,
            current_ts=current_ts,
        )

    Account.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id).delete(synchronize_session=False)


@atomic
def process_account_purge_signal(creditor_id: int, debtor_id: int, creation_date: date) -> None:
    # TODO: Do not foget to do the same thing when the account is dead
    #       (no heartbeat for a long time).

    current_ts = datetime.now(tz=timezone.utc)
    data = AccountData.query.\
        filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            creation_date=creation_date,
            has_server_account=True,
        ).\
        with_for_update().\
        options(load_only(*ACCOUNT_DATA_INFO_RELATED_COLUMNS)).\
        one_or_none()

    if data:
        data.has_server_account = False
        data.principal = 0
        data.interest = 0.0
        data.info_latest_update_id += 1
        data.info_latest_update_ts = current_ts

        db.session.add(PendingLogEntry(
            creditor_id=creditor_id,
            added_at_ts=current_ts,
            object_type=types.account_info,
            object_uri=paths.account_info(creditorId=creditor_id, debtorId=debtor_id),
            object_update_id=data.info_latest_update_id,
        ))


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

    query = db.session.query(AccountData).\
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
        # Note that deleting the `RunningTransfer` record may result
        # in dismissing an already committed transfer. This is not a
        # problem in this case, however, because the user has ordered
        # the deletion of the `DirectTransfer` record, and therefore
        # is not interested in the its outcome.
        RunningTransfer.query.\
            filter_by(debtor_id=debtor_id, transfer_uuid=transfer_uuid).\
            delete(synchronize_session=False)

    return number_of_deleted_rows == 1


def _discard_orphaned_account(
        creditor_id: int,
        debtor_id: int,
        status_flags: int,
        config_flags: int,
        negligible_amount: float) -> None:

    is_scheduled_for_deletion = config_flags & AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    has_huge_negligible_amount = negligible_amount >= DEFAULT_NEGLIGIBLE_AMOUNT
    if not (is_scheduled_for_deletion and has_huge_negligible_amount):
        db.session.add(ConfigureAccountSignal(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            ts=datetime.now(tz=timezone.utc),
            seqnum=0,
            negligible_amount=DEFAULT_NEGLIGIBLE_AMOUNT,
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


def _add_log_entry(
        creditor: Creditor,
        *,
        object_type: str,
        object_uri: str,
        object_update_id: int = None,
        is_deleted: bool = False,
        data: dict = None,
        current_ts: datetime = None) -> None:

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
        object_update_id=object_update_id,
        added_at_ts=current_ts,
        is_deleted=is_deleted,
        data=data,
    ))


def _create_new_account(creditor: Creditor, debtor_id: int, current_ts: datetime) -> Account:
    creditor_id = creditor.creditor_id

    # NOTE: When the new account is created, it will also appear in
    # the list of accounts, so we need to write two events to the log
    # to inform the client about this.
    _add_log_entry(
        creditor,
        object_type=types.account,
        object_uri=paths.account(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=1,
        current_ts=current_ts,
    )
    creditor.account_list_latest_update_id += 1
    creditor.account_list_latest_update_ts = current_ts
    _add_log_entry(
        creditor,
        object_type=types.account_list,
        object_uri=paths.account_list(creditorId=creditor_id),
        object_update_id=creditor.account_list_latest_update_id,
        current_ts=current_ts,
    )

    account = Account(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        created_at_ts=current_ts,
        knowledge=AccountKnowledge(latest_update_ts=current_ts),
        exchange=AccountExchange(latest_update_ts=current_ts),
        display=AccountDisplay(latest_update_ts=current_ts),
        data=AccountData(
            last_config_ts=current_ts,
            last_config_seqnum=0,
            config_latest_update_ts=current_ts,
            info_latest_update_ts=current_ts,
            ledger_latest_update_ts=current_ts,
        ),
        latest_update_ts=current_ts,
    )
    db.session.add(account)

    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=current_ts,
        seqnum=0,
        negligible_amount=DEFAULT_NEGLIGIBLE_AMOUNT,
        config_flags=DEFAULT_CONFIG_FLAGS,
        config='',
    ))

    # NOTE: We must update the way accounts that are pegged to the
    # newly created account are displayed. (And do not forget to write
    # events to the log to inform the client about the changes.)
    account_displays_query = AccountDisplay.query.filter_by(creditor_id=creditor_id, peg_currency_debtor_id=debtor_id)
    for account_display in account_displays_query.all():
        account_display.peg_account_debtor_id = debtor_id
        account_display.latest_update_id += 1
        account_display.latest_update_ts = current_ts
        _add_log_entry(
            creditor,
            object_type=types.account_display,
            object_uri=paths.account_display(creditorId=creditor_id, debtorId=account_display.debtor_id),
            object_update_id=account_display.latest_update_id,
            current_ts=current_ts,
        )

    return account
