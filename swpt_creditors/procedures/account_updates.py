from datetime import datetime, date, timezone, timedelta
from typing import TypeVar, Callable, Tuple, List, Optional
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import exc, Load
from swpt_lib.utils import Seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import AccountData, ConfigureAccountSignal, \
    LogEntry, PendingLogEntry, PendingLedgerUpdate, LedgerEntry, CommittedTransfer, \
    HUGE_NEGLIGIBLE_AMOUNT, DEFAULT_CONFIG_FLAGS, MIN_INT64, MAX_INT64
from .common import ACCOUNT_DATA_LEDGER_RELATED_COLUMNS, LOAD_ONLY_CONFIG_RELATED_COLUMNS, \
    LOAD_ONLY_INFO_RELATED_COLUMNS
from .common import contain_principal_overflow
from .creditors import _is_correct_creditor_id
from .accounts import _insert_info_update_pending_log_entry
from .transfers import ensure_pending_ledger_update

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5
HUGE_INTERVAL = timedelta(days=500000)


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
        last_config_ts == data.last_config_ts
        and last_config_seqnum == data.last_config_seqnum
        and config_flags == data.config_flags
        and config == data.config
        and abs(data.negligible_amount - negligible_amount) <= EPS * negligible_amount
    )
    config_error = None if is_config_effectual else data.config_error
    is_new_server_account = creation_date > data.creation_date
    is_info_updated = (
        data.is_deletion_safe
        or data.account_id != account_id
        or abs(data.interest_rate - interest_rate) > EPS * interest_rate
        or data.last_interest_rate_change_ts != last_interest_rate_change_ts
        or data.transfer_note_max_bytes != transfer_note_max_bytes
        or data.debtor_info_iri != debtor_info_iri
        or data.config_error != config_error
    )
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
    data.last_transfer_committed_at = last_transfer_committed_at
    data.is_config_effectual = is_config_effectual
    data.config_error = config_error

    if is_info_updated:
        _insert_info_update_pending_log_entry(data, current_ts)

    if is_new_server_account:
        log_entry = _update_ledger(
            data=data,
            transfer_number=0,
            acquired_amount=0,
            principal=0,
            current_ts=current_ts,
        )
        if log_entry:
            db.session.add(log_entry)

        ensure_pending_ledger_update(data.creditor_id, data.debtor_id)


@atomic
def process_account_purge_signal(debtor_id: int, creditor_id: int, creation_date: date) -> None:
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
def process_pending_ledger_update(creditor_id: int, debtor_id: int, max_count: int, max_delay: timedelta) -> bool:
    """Try to add pending committed transfers to the account's ledger.

    This function will not process more than `max_count`
    transfers. When some legible committed transfers remained
    unprocessed, `False` will be returned. In this case the function
    should be called again, and again, until it returns `True`.

    When one or more `AccountTransfer` messages have been lost, after
    some time (determined by the `max_delay` attribute), the account's
    ledger will be automatically "repaired", and the lost transfers
    skipped.

    """

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

    log_entry = None
    committed_at_cutoff = current_ts - max_delay
    transfers = _get_sorted_pending_transfers(data, max_count)
    for previous_transfer_number, transfer_number, acquired_amount, principal, committed_at in transfers:
        if previous_transfer_number != data.ledger_last_transfer_number and committed_at >= committed_at_cutoff:
            data.ledger_pending_transfer_ts = committed_at
            is_done = True
            break
        log_entry = _update_ledger(
            data=data,
            transfer_number=transfer_number,
            acquired_amount=acquired_amount,
            principal=principal,
            current_ts=current_ts,
        ) or log_entry
    else:
        data.ledger_pending_transfer_ts = None
        is_done = len(transfers) < max_count

    if is_done:
        log_entry = _fix_missing_last_transfer_if_necessary(data, max_delay, current_ts) or log_entry
        db.session.delete(pending_ledger_update)

    if log_entry:
        db.session.add(log_entry)

    return is_done


def _get_sorted_pending_transfers(data: AccountData, max_count: int) -> List[Tuple]:
    return db.session.\
        query(
            CommittedTransfer.previous_transfer_number,
            CommittedTransfer.transfer_number,
            CommittedTransfer.acquired_amount,
            CommittedTransfer.principal,
            CommittedTransfer.committed_at,
        ).\
        filter(
            CommittedTransfer.creditor_id == data.creditor_id,
            CommittedTransfer.debtor_id == data.debtor_id,
            CommittedTransfer.creation_date == data.creation_date,
            CommittedTransfer.transfer_number > data.ledger_last_transfer_number,
        ).\
        order_by(CommittedTransfer.transfer_number).\
        limit(max_count).\
        all()


def _fix_missing_last_transfer_if_necessary(
        data: AccountData,
        max_delay: timedelta,
        current_ts: datetime) -> Optional[PendingLogEntry]:

    has_no_pending_transfers = data.ledger_pending_transfer_ts is None
    last_transfer_is_missing = data.last_transfer_number > data.ledger_last_transfer_number
    last_transfer_is_old = data.last_transfer_committed_at < current_ts - max_delay
    if has_no_pending_transfers and last_transfer_is_missing and last_transfer_is_old:
        return _update_ledger(
            data=data,
            transfer_number=data.last_transfer_number,
            acquired_amount=0,
            principal=data.principal,
            current_ts=current_ts,
        )


def _discard_orphaned_account(creditor_id: int, debtor_id: int, config_flags: int, negligible_amount: float) -> None:
    if _is_correct_creditor_id(creditor_id):
        scheduled_for_deletion_flag = AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        safely_huge_amount = (1 - EPS) * HUGE_NEGLIGIBLE_AMOUNT  # slightly smaller than `HUGE_NEGLIGIBLE_AMOUNT`
        if not (config_flags & scheduled_for_deletion_flag and negligible_amount >= safely_huge_amount):
            db.session.add(ConfigureAccountSignal(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                ts=datetime.now(tz=timezone.utc),
                seqnum=0,
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
                config_flags=DEFAULT_CONFIG_FLAGS | scheduled_for_deletion_flag,
            ))


def _update_ledger(
        data: AccountData,
        transfer_number: int,
        acquired_amount: int,
        principal: int,
        current_ts: datetime) -> Optional[PendingLogEntry]:

    should_insert_ledger_update_log_entry = _make_correcting_ledger_entry_if_necessary(
        data=data,
        acquired_amount=acquired_amount,
        principal=principal,
        current_ts=current_ts,
    )

    if acquired_amount != 0:
        data.ledger_last_entry_id += 1
        db.session.add(LedgerEntry(
            creditor_id=data.creditor_id,
            debtor_id=data.debtor_id,
            entry_id=data.ledger_last_entry_id,
            aquired_amount=acquired_amount,
            principal=principal,
            added_at=current_ts,
            creation_date=data.creation_date,
            transfer_number=transfer_number,
        ))
        should_insert_ledger_update_log_entry = True

    assert should_insert_ledger_update_log_entry or data.ledger_principal == principal
    data.ledger_principal = principal
    data.ledger_last_transfer_number = transfer_number
    data.ledger_pending_transfer_ts = None

    if should_insert_ledger_update_log_entry:
        data.ledger_latest_update_id += 1
        data.ledger_latest_update_ts = current_ts

        return PendingLogEntry(
            creditor_id=data.creditor_id,
            added_at=current_ts,
            object_type_hint=LogEntry.OTH_ACCOUNT_LEDGER,
            debtor_id=data.debtor_id,
            object_update_id=data.ledger_latest_update_id,
            data_principal=principal,
            data_next_entry_id=data.ledger_last_entry_id + 1,
        )


def _make_correcting_ledger_entry_if_necessary(
        data: AccountData,
        acquired_amount: int,
        principal: int,
        current_ts: datetime) -> bool:

    made_correcting_ledger_entry = False

    previous_principal = principal - acquired_amount
    if MIN_INT64 <= previous_principal <= MAX_INT64:
        ledger_principal = data.ledger_principal
        correction_amount = previous_principal - ledger_principal

        while correction_amount != 0:
            safe_correction_amount = contain_principal_overflow(correction_amount)
            correction_amount -= safe_correction_amount
            ledger_principal += safe_correction_amount

            data.ledger_last_entry_id += 1
            db.session.add(LedgerEntry(
                creditor_id=data.creditor_id,
                debtor_id=data.debtor_id,
                entry_id=data.ledger_last_entry_id,
                aquired_amount=safe_correction_amount,
                principal=ledger_principal,
                added_at=current_ts,
            ))
            made_correcting_ledger_entry = True

    return made_correcting_ledger_entry
