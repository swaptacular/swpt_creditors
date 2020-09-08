from datetime import datetime, date, timezone, timedelta
from typing import TypeVar, Callable, Tuple, List, Optional
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import exc, Load
from swpt_lib.utils import Seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import AccountData, ConfigureAccountSignal, \
    PendingLogEntry, PendingLedgerUpdate, LedgerEntry, CommittedTransfer, \
    TRANSFER_NOTE_MAX_BYTES, HUGE_NEGLIGIBLE_AMOUNT, DEFAULT_CONFIG_FLAGS
from .common import get_paths_and_types, LOAD_ONLY_CONFIG_RELATED_COLUMNS, \
    LOAD_ONLY_INFO_RELATED_COLUMNS, ACCOUNT_DATA_LEDGER_RELATED_COLUMNS
from .creditors import _is_correct_creditor_id
from .accounts import _insert_info_update_pending_log_entry
from .transfers import _ensure_pending_ledger_update

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5
HUGE_INTERVAL = timedelta(days=500000)
assert 0 < EPS <= 0.01


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
        options(LOAD_ONLY_CONFIG_RELATED_COLUMNS).\
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

    data = AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        with_for_update().\
        one_or_none()

    if data is None:
        _discard_orphaned_account(creditor_id, debtor_id, config_flags, negligible_amount)
        return

    if ts > data.last_heartbeat_ts:
        data.last_heartbeat_ts = min(ts, current_ts)

    prev_event = (data.creation_date, data.last_change_ts, Seqnum(data.last_change_seqnum))
    this_event = (creation_date, last_change_ts, Seqnum(last_change_seqnum))
    if this_event <= prev_event:
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
    is_info_updated = (
        data.is_deletion_safe
        or data.account_id != account_id
        or abs(data.interest_rate - interest_rate) > EPS * interest_rate
        or data.last_interest_rate_change_ts != last_interest_rate_change_ts
        or data.transfer_note_max_bytes != transfer_note_max_bytes
        or data.debtor_info_iri != debtor_info_iri
        or data.config_error != config_error
    )
    if is_info_updated:
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
        ledger_update_pending_log_entry = _insert_ledger_entry(
            data=data,
            transfer_number=0,
            acquired_amount=0,
            principal=0,
            committed_at_ts=data.ledger_last_transfer_committed_at_ts,
            current_ts=current_ts,
        )
        if ledger_update_pending_log_entry:
            db.session.add(ledger_update_pending_log_entry)

        _ensure_pending_ledger_update(data.creditor_id, data.debtor_id)


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
        options(LOAD_ONLY_INFO_RELATED_COLUMNS).\
        one_or_none()

    if data:
        data.has_server_account = False
        data.principal = 0
        data.interest = 0.0
        _insert_info_update_pending_log_entry(data, current_ts)


@atomic
def get_pending_ledger_updates(max_count: int = None) -> List[Tuple[int, int]]:
    query = db.session.query(PendingLedgerUpdate.creditor_id, PendingLedgerUpdate.debtor_id)
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_pending_ledger_update(
        creditor_id: int,
        debtor_id: int,
        *,
        max_count: int = None,
        max_delay: timedelta = HUGE_INTERVAL) -> bool:

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
    ledger_update_pending_log_entry = None
    committed_at_cutoff = current_ts - max_delay
    for previous_transfer_number, transfer_number, acquired_amount, principal, committed_at_ts in transfers:
        if previous_transfer_number != data.ledger_last_transfer_number and committed_at_ts >= committed_at_cutoff:
            all_done = True
            break
        e = _insert_ledger_entry(data, transfer_number, acquired_amount, principal, committed_at_ts, current_ts)
        ledger_update_pending_log_entry = e or ledger_update_pending_log_entry

    if ledger_update_pending_log_entry:
        db.session.add(ledger_update_pending_log_entry)

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


def _discard_orphaned_account(creditor_id: int, debtor_id: int, config_flags: int, negligible_amount: float) -> None:
    if _is_correct_creditor_id(creditor_id):
        scheduled_for_deletion_flag = AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        safely_huge_amount = (1 - EPS) * HUGE_NEGLIGIBLE_AMOUNT
        assert safely_huge_amount < HUGE_NEGLIGIBLE_AMOUNT
        if not (config_flags & scheduled_for_deletion_flag and negligible_amount >= safely_huge_amount):
            db.session.add(ConfigureAccountSignal(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                ts=datetime.now(tz=timezone.utc),
                seqnum=0,
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
                config_flags=DEFAULT_CONFIG_FLAGS | scheduled_for_deletion_flag,
            ))


def _insert_ledger_entry(
        data: AccountData,
        transfer_number: int,
        acquired_amount: int,
        principal: int,
        committed_at_ts: datetime,
        current_ts: datetime) -> Optional[PendingLogEntry]:

    ledger_update_pending_log_entry = None
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
        data.ledger_latest_update_id += 1
        data.ledger_latest_update_ts = current_ts
        paths, types = get_paths_and_types()
        ledger_update_pending_log_entry = PendingLogEntry(
            creditor_id=creditor_id,
            added_at_ts=current_ts,
            object_type=types.account_ledger,
            object_uri=paths.account_ledger(creditorId=creditor_id, debtorId=debtor_id),
            object_update_id=data.ledger_latest_update_id,
            data_principal=principal,
            data_next_entry_id=data.ledger_last_entry_id + 1,
        )

    return ledger_update_pending_log_entry
