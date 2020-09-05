from datetime import datetime, date, timezone
from typing import TypeVar, Callable, Tuple, List, Optional
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import joinedload, exc, load_only, Load
from swpt_lib.utils import Seqnum, increment_seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    Account, AccountData,
    ConfigureAccountSignal, AccountDisplay, AccountExchange, AccountKnowledge,
    PendingLogEntry, PendingLedgerUpdate, LedgerEntry, CommittedTransfer,
    MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, TRANSFER_NOTE_MAX_BYTES,
    DEFAULT_NEGLIGIBLE_AMOUNT, DEFAULT_CONFIG_FLAGS,
)
from .common import (
    allow_update, get_paths_and_types,
    ACCOUNT_DATA_CONFIG_RELATED_COLUMNS, ACCOUNT_DATA_LEDGER_RELATED_COLUMNS,
    ACCOUNT_DATA_INFO_RELATED_COLUMNS,
)
from .creditors import has_account
from . import errors

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5


@atomic
def get_account(creditor_id: int, debtor_id: int) -> Optional[Account]:
    options = [
        joinedload(Account.knowledge, innerjoin=True),
        joinedload(Account.exchange, innerjoin=True),
        joinedload(Account.display, innerjoin=True),
        joinedload(Account.data, innerjoin=True),
    ]
    return Account.get_instance((creditor_id, debtor_id), *options)


@atomic
def get_account_info(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(load_only(*ACCOUNT_DATA_INFO_RELATED_COLUMNS)).\
        one_or_none()


@atomic
def get_account_ledger(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(load_only(*ACCOUNT_DATA_LEDGER_RELATED_COLUMNS)).\
        one_or_none()


@atomic
def get_account_ledger_entries(
        creditor_id: int,
        debtor_id: int,
        *,
        prev: int,
        stop: int = 0,
        count: int = 1) -> List[LedgerEntry]:

    assert 0 <= prev <= MAX_INT64
    assert 0 <= stop <= MAX_INT64
    assert count >= 1

    return LedgerEntry.query.\
        filter(
            LedgerEntry.creditor_id == creditor_id,
            LedgerEntry.debtor_id == debtor_id,
            LedgerEntry.entry_id < prev,
            LedgerEntry.entry_id > stop,
        ).\
        order_by(LedgerEntry.entry_id.desc()).\
        limit(count).\
        all()


@atomic
def get_account_config(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)).\
        one_or_none()


@atomic
def update_account_config(
        creditor_id: int,
        debtor_id: int,
        *,
        is_scheduled_for_deletion: bool,
        negligible_amount: float,
        allow_unsafe_deletion: bool,
        latest_update_id: int) -> AccountData:

    current_ts = datetime.now(tz=timezone.utc)
    options = [load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)]
    data = AccountData.lock_instance((creditor_id, debtor_id), *options)
    if data is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(data, 'config_latest_update_id', latest_update_id, {
            'is_scheduled_for_deletion': is_scheduled_for_deletion,
            'negligible_amount': negligible_amount,
            'allow_unsafe_deletion': allow_unsafe_deletion,
        })
    except errors.AlreadyUpToDate:
        return data

    # NOTE: The account will not be safe to delete once the config is
    # updated. Therefore, if the account is safe to delete now, we
    # need to inform the client about the upcoming change.
    if data.is_deletion_safe:
        _insert_info_update_pending_log_entry(data, current_ts)

    data.last_config_ts = current_ts
    data.last_config_seqnum = increment_seqnum(data.last_config_seqnum)
    data.is_config_effectual = False
    data.config_latest_update_ts = current_ts
    perform_update()

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_config,
        object_uri=paths.account_config(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))
    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=data.last_config_ts,
        seqnum=data.last_config_seqnum,
        negligible_amount=data.negligible_amount,
        config_flags=data.config_flags,
    ))

    return data


@atomic
def get_account_display(creditor_id: int, debtor_id: int) -> Optional[AccountDisplay]:
    return AccountDisplay.get_instance((creditor_id, debtor_id))


@atomic
def update_account_display(
        creditor_id: int,
        debtor_id: int,
        *,
        debtor_name: Optional[str],
        amount_divisor: float,
        decimal_places: int,
        unit: Optional[str],
        hide: bool,
        latest_update_id: int) -> AccountDisplay:

    assert amount_divisor > 0.0
    assert MIN_INT32 <= decimal_places <= MAX_INT32
    assert 1 <= latest_update_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    display = AccountDisplay.lock_instance((creditor_id, debtor_id))
    if display is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(display, 'latest_update_id', latest_update_id, {
            'debtor_name': debtor_name,
            'amount_divisor': amount_divisor,
            'decimal_places': decimal_places,
            'unit': unit,
            'hide': hide,
        })
    except errors.AlreadyUpToDate:
        return display

    if debtor_name not in [display.debtor_name, None]:
        debtor_name_query = AccountDisplay.query.filter_by(creditor_id=creditor_id, debtor_name=debtor_name)
        if db.session.query(debtor_name_query.exists()).scalar():
            raise errors.DebtorNameConflict()

    with db.retry_on_integrity_error():
        display.latest_update_ts = current_ts
        perform_update()

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_display,
        object_uri=paths.account_display(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))

    return display


@atomic
def get_account_knowledge(creditor_id: int, debtor_id: int) -> Optional[AccountKnowledge]:
    return AccountKnowledge.get_instance((creditor_id, debtor_id))


@atomic
def update_account_knowledge(
        creditor_id: int,
        debtor_id: int,
        *,
        latest_update_id: int,
        data: dict) -> AccountKnowledge:

    current_ts = datetime.now(tz=timezone.utc)
    knowledge = AccountKnowledge.lock_instance((creditor_id, debtor_id))
    if knowledge is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(knowledge, 'latest_update_id', latest_update_id, {'data': data})
    except errors.AlreadyUpToDate:
        return knowledge

    knowledge.latest_update_ts = current_ts
    perform_update()

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_knowledge,
        object_uri=paths.account_knowledge(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))

    return knowledge


@atomic
def get_account_exchange(creditor_id: int, debtor_id: int) -> Optional[AccountExchange]:
    return AccountExchange.get_instance((creditor_id, debtor_id))


@atomic
def update_account_exchange(
        creditor_id: int,
        debtor_id: int,
        *,
        policy: Optional[str],
        min_principal: int,
        max_principal: int,
        peg_exchange_rate: Optional[float],
        peg_debtor_id: Optional[int],
        latest_update_id: int) -> AccountKnowledge:

    if policy not in [None, 'conservative']:
        raise errors.InvalidExchangePolicy()

    current_ts = datetime.now(tz=timezone.utc)
    exchange = AccountExchange.lock_instance((creditor_id, debtor_id))
    if exchange is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(exchange, 'latest_update_id', latest_update_id, {
            'policy': policy,
            'min_principal': min_principal,
            'max_principal': max_principal,
            'peg_exchange_rate': peg_exchange_rate,
            'peg_debtor_id': peg_debtor_id,
        })
    except errors.AlreadyUpToDate:
        return exchange

    if peg_debtor_id is not None and not has_account(creditor_id, peg_debtor_id):
        raise errors.PegDoesNotExist()

    exchange.latest_update_ts = current_ts
    perform_update()

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_exchange,
        object_uri=paths.account_exchange(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))

    return exchange


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
    # TODO: Do not foget to do the same thing when the account is dead
    #       (no heartbeat for a long time).

    current_ts = datetime.now(tz=timezone.utc)
    data = AccountData.query.\
        filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            has_server_account=True,
        ).\
        filter(AccountData.creation_date <= creation_date).\
        with_for_update().\
        options(load_only(*ACCOUNT_DATA_INFO_RELATED_COLUMNS)).\
        one_or_none()

    if data:
        data.has_server_account = False
        data.principal = 0
        data.interest = 0.0
        _insert_info_update_pending_log_entry(data, current_ts)


@atomic
def process_rejected_config_signal(
        debtor_id: int,
        creditor_id: int,
        config_ts: datetime,
        config_seqnum: int,
        negligible_amount: float,
        config: str,
        config_flags: int,
        rejection_code: str) -> None:

    assert rejection_code == '' or len(rejection_code) <= 30 and rejection_code.encode('ascii')

    if config != '':
        return

    current_ts = datetime.now(tz=timezone.utc)
    data = AccountData.query.\
        filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            last_config_ts=config_ts,
            last_config_seqnum=config_seqnum,
            config_flags=config_flags,
            config_error=None,
        ).\
        filter(func.abs(AccountData.negligible_amount - negligible_amount) <= EPS * negligible_amount).\
        with_for_update().\
        options(load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)).\
        one_or_none()

    if data:
        data.config_error = rejection_code
        _insert_info_update_pending_log_entry(data, current_ts)


@atomic
def process_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_interest_rate_change_ts: datetime,
        transfer_note_max_bytes: int,
        status_flags: int,
        last_config_ts: datetime,
        last_config_seqnum: int,
        negligible_amount: float,
        config_flags: int,
        config: str,
        account_id: str,
        debtor_info_iri: str,
        last_transfer_number: int,
        last_transfer_committed_at: datetime,
        ts: datetime,
        ttl: int) -> None:

    assert 0 <= transfer_note_max_bytes <= TRANSFER_NOTE_MAX_BYTES
    assert account_id == '' or len(account_id) <= 100 and account_id.encode('ascii')
    assert len(debtor_info_iri) <= 200

    # TODO: Think about limiting the maximum rate at which this
    #       procedure can be called. Calling it too often may lead to
    #       lock contention on `AccountData` rows, although it is not
    #       clear if this is a practical problem.

    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    data = AccountData.lock_instance((creditor_id, debtor_id))
    if data is None:
        _discard_orphaned_account(creditor_id, debtor_id, config_flags, negligible_amount)
        return

    if ts > data.last_heartbeat_ts:
        data.last_heartbeat_ts = min(ts, current_ts)

    prev_event = (data.creation_date, data.last_change_ts, Seqnum(data.last_change_seqnum))
    this_event = (creation_date, last_change_ts, Seqnum(last_change_seqnum))
    is_new_event = this_event > prev_event
    if not is_new_event:
        return

    is_config_effectual = (
        config == ''
        and last_config_ts == data.last_config_ts
        and last_config_seqnum == data.last_config_seqnum
        and config_flags == data.config_flags
        and abs(data.negligible_amount - negligible_amount) <= EPS * negligible_amount
    )
    config_error = None if is_config_effectual else data.config_error
    new_server_account = creation_date > data.creation_date
    info_update = (
        data.is_deletion_safe
        or data.account_id != account_id
        or abs(data.interest_rate - interest_rate) > EPS * interest_rate
        or data.last_interest_rate_change_ts != last_interest_rate_change_ts
        or data.transfer_note_max_bytes != transfer_note_max_bytes
        or data.debtor_info_iri != debtor_info_iri
        or data.config_error != config_error
    )
    if info_update:
        _insert_info_update_pending_log_entry(data, current_ts)

    data.has_server_account = True
    data.creation_date = creation_date
    data.last_change_ts = last_change_ts
    data.last_change_seqnum = last_change_seqnum
    data.principal = principal
    data.interest = interest
    data.interest_rate = interest_rate
    data.last_interest_rate_change_ts = last_interest_rate_change_ts
    data.transfer_note_max_bytes = transfer_note_max_bytes
    data.status_flags = status_flags
    data.account_id = account_id
    data.debtor_info_iri = debtor_info_iri
    data.last_transfer_number = last_transfer_number,
    data.last_transfer_committed_at_ts = last_transfer_committed_at
    data.is_config_effectual = is_config_effectual
    data.config_error = config_error

    if new_server_account:
        _insert_ledger_entry(data, 0, 0, 0, data.ledger_last_transfer_committed_at_ts, current_ts)
        ensure_pending_ledger_update(data.creditor_id, data.debtor_id)


@atomic
def ensure_pending_ledger_update(creditor_id: int, debtor_id: int) -> None:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    pending_ledger_update_query = PendingLedgerUpdate.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    if not db.session.query(pending_ledger_update_query.exists()).scalar():
        with db.retry_on_integrity_error():
            db.session.add(PendingLedgerUpdate(creditor_id=creditor_id, debtor_id=debtor_id))


@atomic
def get_pending_ledger_updates(max_count: int = None) -> List[Tuple[int, int]]:
    query = db.session.query(PendingLedgerUpdate.creditor_id, PendingLedgerUpdate.debtor_id)
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_pending_ledger_update(creditor_id: int, debtor_id: int, max_count: int = None) -> bool:
    """Returns `False` if some legible committed transfers remained unprocessed."""

    current_ts = datetime.now(tz=timezone.utc)
    query = db.session.\
        query(PendingLedgerUpdate, AccountData).\
        join(PendingLedgerUpdate.account_data).\
        filter(PendingLedgerUpdate.creditor_id == creditor_id, PendingLedgerUpdate.debtor_id == debtor_id).\
        with_for_update().\
        options(Load(AccountData).load_only(*ACCOUNT_DATA_LEDGER_RELATED_COLUMNS))
    try:
        pending_ledger_update, data = query.one()
    except exc.NoResultFound:
        return True

    transfers = _get_sorted_pending_transfers(data, max_count)
    all_done = max_count is None or len(transfers) < max_count
    for previous_transfer_number, transfer_number, acquired_amount, principal, committed_at_ts in transfers:
        if previous_transfer_number != data.ledger_last_transfer_number:
            all_done = True
            break
        _insert_ledger_entry(data, transfer_number, acquired_amount, principal, committed_at_ts, current_ts)

    if all_done:
        db.session.delete(pending_ledger_update)
    return all_done


def _get_sorted_pending_transfers(data: AccountData, max_count: int = None) -> List[Tuple]:
    transfer_numbers_query = db.session.\
        query(
            CommittedTransfer.previous_transfer_number,
            CommittedTransfer.transfer_number,
            CommittedTransfer.acquired_amount,
            CommittedTransfer.principal,
            CommittedTransfer.committed_at_ts,
        ).\
        filter(
            CommittedTransfer.creditor_id == data.creditor_id,
            CommittedTransfer.debtor_id == data.debtor_id,
            CommittedTransfer.creation_date == data.creation_date,
            CommittedTransfer.transfer_number > data.ledger_last_transfer_number,
        ).\
        order_by(CommittedTransfer.transfer_number)

    if max_count is not None:
        transfer_numbers_query = transfer_numbers_query.limit(max_count)

    return transfer_numbers_query.all()


def _insert_info_update_pending_log_entry(data: AccountData, current_ts: datetime) -> None:
    creditor_id = data.creditor_id
    debtor_id = data.debtor_id

    data.info_latest_update_id += 1
    data.info_latest_update_ts = current_ts
    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.account_info,
        object_uri=paths.account_info(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=data.info_latest_update_id,
    ))


def _discard_orphaned_account(creditor_id: int, debtor_id: int, config_flags: int, negligible_amount: float) -> None:
    # TODO: Consider consulting the `CreditorSpace` table before
    #       performing this potentially very dangerous
    #       operation. Also, consider adding a "recovery" app
    #       configuration option, and if it is set, do not delete
    #       orphaned accounts, but instead create creditor accounts.

    scheduled_for_deletion_flag = AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    if not (config_flags & scheduled_for_deletion_flag and negligible_amount >= DEFAULT_NEGLIGIBLE_AMOUNT):
        db.session.add(ConfigureAccountSignal(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            ts=datetime.now(tz=timezone.utc),
            seqnum=0,
            negligible_amount=DEFAULT_NEGLIGIBLE_AMOUNT,
            config_flags=DEFAULT_CONFIG_FLAGS | scheduled_for_deletion_flag,
        ))


def _insert_ledger_entry(
        data: AccountData,
        transfer_number: int,
        acquired_amount: int,
        principal: int,
        committed_at_ts: datetime,
        current_ts: datetime) -> None:

    creditor_id = data.creditor_id
    debtor_id = data.debtor_id
    correction_amount = principal - data.ledger_principal - acquired_amount

    if correction_amount != 0:
        data.ledger_last_entry_id += 1
        db.session.add(LedgerEntry(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            entry_id=data.ledger_last_entry_id,
            aquired_amount=correction_amount,
            principal=principal - acquired_amount,
            added_at_ts=current_ts,
        ))

    if acquired_amount != 0:
        data.ledger_last_entry_id += 1
        db.session.add(LedgerEntry(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            entry_id=data.ledger_last_entry_id,
            aquired_amount=acquired_amount,
            principal=principal,
            added_at_ts=current_ts,
            creation_date=data.creation_date,
            transfer_number=transfer_number,
        ))

    data.ledger_principal = principal
    data.ledger_last_transfer_number = transfer_number
    data.ledger_last_transfer_committed_at_ts = committed_at_ts

    if correction_amount != 0 or acquired_amount != 0:
        # TODO: Use bulk insert for adding `PendingLogEntry`s. Now
        #       each added row causes a database roundtrip to load the
        #       auto-incremented primary key.

        # TODO: Add `data`, containing the principal and the latest
        #       etnry ID.

        data.ledger_latest_update_id += 1
        data.ledger_latest_update_ts = current_ts
        paths, types = get_paths_and_types()
        db.session.add(PendingLogEntry(
            creditor_id=creditor_id,
            added_at_ts=current_ts,
            object_type=types.account_ledger,
            object_uri=paths.account_ledger(creditorId=creditor_id, debtorId=debtor_id),
            object_update_id=data.ledger_latest_update_id,
        ))
