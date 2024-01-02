from typing import TypeVar, Callable, List, Tuple, Optional, Iterable
from datetime import datetime, timezone, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import joinedload
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    MIN_INT64,
    MAX_INT64,
    DATE0,
    DEFAULT_CONFIG_FLAGS,
    Creditor,
    LogEntry,
    PendingLogEntry,
    PinInfo,
    Account,
    RunningTransfer,
    UpdatedLedgerSignal,
    UpdatedFlagsSignal,
    UpdatedPolicySignal,
    uid_seq,
)
from .common import get_paths_and_types
from . import errors

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

ACTIVATION_STATUS_MASK = (
    Creditor.STATUS_IS_ACTIVATED_FLAG | Creditor.STATUS_IS_DEACTIVATED_FLAG
)
LOG_ENTRY_NONE_AUX_FIELDS_EXCLUDED_TYPE_HINT = {
    attr: None for attr in LogEntry.AUX_FIELDS if attr != "object_type_hint"
}
LOG_ENTRY_NONE_DATA_FIELDS = {attr: None for attr in LogEntry.DATA_FIELDS}


def verify_pin_value(
    creditor_id: int,
    *,
    secret: str,
    pin_value: Optional[str],
    pin_failures_reset_interval: timedelta
) -> None:
    is_pin_value_ok = verify_pin_value_helper(
        creditor_id,
        secret=secret,
        pin_value=pin_value,
        pin_failures_reset_interval=pin_failures_reset_interval,
    )
    if not is_pin_value_ok:
        raise errors.WrongPinValue()


def update_pin_info(
    creditor_id: int,
    *,
    status_name: str,
    secret: str,
    new_pin_value: Optional[str],
    latest_update_id: int,
    pin_reset_mode: bool,
    pin_value: Optional[str],
    pin_failures_reset_interval: timedelta
) -> Optional[PinInfo]:
    is_pin_value_ok, pin_info = update_pin_info_helper(
        creditor_id=creditor_id,
        status_name=status_name,
        secret=secret,
        new_pin_value=new_pin_value,
        latest_update_id=latest_update_id,
        pin_reset_mode=pin_reset_mode,
        pin_value=pin_value,
        pin_failures_reset_interval=pin_failures_reset_interval,
    )
    if not is_pin_value_ok:
        raise errors.WrongPinValue()

    return pin_info


@atomic
def get_creditor_ids(
    start_from: int, count: int = 1
) -> Tuple[List[int], Optional[int]]:
    assert count >= 1
    query = (
        db.session.query(Creditor.creditor_id)
        .filter(Creditor.creditor_id >= start_from)
        .filter(
            Creditor.status_flags.op("&")(ACTIVATION_STATUS_MASK)
            == Creditor.STATUS_IS_ACTIVATED_FLAG
        )
        .order_by(Creditor.creditor_id)
        .limit(count + 1)
    )
    creditor_ids = [t[0] for t in query.all()]

    if len(creditor_ids) > count:
        next_creditor_id = creditor_ids.pop()
    else:
        next_creditor_id = None

    return creditor_ids, next_creditor_id


@atomic
def reserve_creditor(creditor_id) -> Creditor:
    creditor = Creditor(creditor_id=creditor_id)
    db.session.add(creditor)
    try:
        db.session.flush()
    except IntegrityError:
        raise errors.CreditorExists() from None

    relic_log_entry_id = (
        db.session.query(func.max(LogEntry.entry_id))
        .filter_by(creditor_id=creditor_id)
        .scalar()
    )
    creditor.last_log_entry_id = (
        0 if relic_log_entry_id is None else relic_log_entry_id + 1
    )  # a gap

    return creditor


@atomic
def activate_creditor(creditor_id: int, reservation_id: str) -> Creditor:
    creditor = _get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise errors.InvalidReservationId()

    if not creditor.is_activated:
        if (
            reservation_id != str(creditor.reservation_id)
            or creditor.is_deactivated
        ):
            raise errors.InvalidReservationId()

        creditor.activate()
        db.session.add(PinInfo(creditor_id=creditor_id))

    return creditor


@atomic
def deactivate_creditor(creditor_id: int) -> None:
    creditor = get_active_creditor(creditor_id, lock=True)
    if creditor:
        creditor.deactivate()
        _delete_creditor_pin_info(creditor_id)
        _delete_creditor_accounts(creditor_id)
        _delete_creditor_running_transfers(creditor_id)


@atomic
def get_active_creditor(
    creditor_id: int, lock: bool = False, join_pin: bool = False
) -> Optional[Creditor]:
    creditor = _get_creditor(creditor_id, lock=lock, join_pin=join_pin)
    if creditor and creditor.is_activated and not creditor.is_deactivated:
        return creditor


@atomic
def get_pin_info(creditor_id: int, lock: bool = False) -> Optional[PinInfo]:
    query = PinInfo.query.filter_by(creditor_id=creditor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def update_pin_info_helper(
    creditor_id: int,
    *,
    status_name: str,
    secret: str,
    new_pin_value: Optional[str],
    latest_update_id: int,
    pin_reset_mode: bool,
    pin_value: Optional[str],
    pin_failures_reset_interval: timedelta
) -> Tuple[bool, PinInfo]:
    current_ts = datetime.now(tz=timezone.utc)

    pin_info = get_pin_info(creditor_id, lock=True)
    if pin_info is None:
        raise errors.CreditorDoesNotExist()

    if latest_update_id != pin_info.latest_update_id + 1:
        raise errors.UpdateConflict()

    is_pin_value_ok = pin_reset_mode or pin_info.try_value(
        pin_value, secret, pin_failures_reset_interval
    )
    if is_pin_value_ok:
        pin_info.status_name = status_name
        pin_info.set_value(new_pin_value, secret)
        pin_info.latest_update_id = latest_update_id
        pin_info.latest_update_ts = current_ts

        paths, types = get_paths_and_types()
        db.session.add(
            PendingLogEntry(
                creditor_id=creditor_id,
                added_at=current_ts,
                object_type=types.pin_info,
                object_uri=paths.pin_info(creditorId=creditor_id),
                object_update_id=latest_update_id,
            )
        )

    return is_pin_value_ok, pin_info


@atomic
def get_log_entries(
    creditor_id: int, *, count: int = 1, prev: int = 0
) -> Tuple[List[LogEntry], int]:
    last_log_entry_id = (
        db.session.query(Creditor.last_log_entry_id)
        .filter(Creditor.creditor_id == creditor_id)
        .scalar()
    )

    if last_log_entry_id is None:
        raise errors.CreditorDoesNotExist()

    log_entries = (
        LogEntry.query.filter(LogEntry.creditor_id == creditor_id)
        .filter(LogEntry.entry_id > prev)
        .order_by(LogEntry.entry_id)
        .limit(count)
        .all()
    )

    return log_entries, last_log_entry_id


@atomic
def get_creditors_with_pending_log_entries(
    max_count: int = None,
) -> Iterable[Tuple[int]]:
    query = db.session.query(PendingLogEntry.creditor_id).distinct()
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_pending_log_entries(creditor_id: int) -> None:
    creditor = _get_creditor(creditor_id, lock=True)
    if creditor:
        pending_log_entries = (
            PendingLogEntry.query.filter_by(creditor_id=creditor_id)
            .order_by(PendingLogEntry.pending_entry_id)
            .with_for_update()
            .all()
        )

        for entry in pending_log_entries:
            _process_pending_log_entry(creditor, entry)


@atomic
def verify_pin_value_helper(
    creditor_id: int,
    *,
    secret: str,
    pin_value: Optional[str],
    pin_failures_reset_interval: timedelta
) -> bool:
    pin_info = get_pin_info(creditor_id)
    if pin_info is None:
        raise errors.CreditorDoesNotExist()

    return pin_info.try_value(pin_value, secret, pin_failures_reset_interval)


def _process_pending_log_entry(
    creditor: Creditor, entry: PendingLogEntry
) -> None:
    paths, types = get_paths_and_types()
    aux_fields = {attr: getattr(entry, attr) for attr in LogEntry.AUX_FIELDS}
    data_fields = {attr: getattr(entry, attr) for attr in LogEntry.DATA_FIELDS}

    _add_log_entry(
        creditor,
        object_type=entry.object_type,
        object_uri=entry.object_uri,
        object_update_id=entry.object_update_id,
        added_at=entry.added_at,
        is_deleted=entry.is_deleted,
        data=entry.data,
        **aux_fields,
        **data_fields,
    )

    if entry.get_object_type(types) == types.transfer and (
        entry.is_created or entry.is_deleted
    ):
        # NOTE: When a running transfer has been created or deleted,
        # the client should be informed about the update in his list
        # of transfers. The write to the log is performed now, because
        # at the time the running transfer was created/deleted, the
        # correct value of the `object_update_id` field had been
        # unknown (a lock on creditor's table row would be required).
        _add_transfers_list_update_log_entry(creditor, entry.added_at)

    db.session.delete(entry)


def _add_transfers_list_update_log_entry(
    creditor: Creditor, added_at: datetime
) -> None:
    paths, types = get_paths_and_types()
    creditor.transfers_list_latest_update_id += 1
    creditor.transfers_list_latest_update_ts = added_at

    _add_log_entry(
        creditor,
        object_type=None,
        object_uri=None,
        object_update_id=creditor.transfers_list_latest_update_id,
        added_at=creditor.transfers_list_latest_update_ts,
        is_deleted=None,
        data=None,
        **LOG_ENTRY_NONE_DATA_FIELDS,
        **LOG_ENTRY_NONE_AUX_FIELDS_EXCLUDED_TYPE_HINT,
        object_type_hint=LogEntry.OTH_TRANSFERS_LIST,
    )


def _get_creditor(
    creditor_id: int, lock: bool = False, join_pin: bool = False
) -> Optional[Creditor]:
    query = Creditor.query.filter_by(creditor_id=creditor_id)
    if lock:
        query = query.with_for_update()
    if join_pin:
        query = query.options(joinedload(Creditor.pin_info, innerjoin=True))

    return query.one_or_none()


def _add_log_entry(creditor: Creditor, **kwargs) -> None:
    db.session.add(
        LogEntry(
            creditor_id=creditor.creditor_id,
            entry_id=creditor.generate_log_entry_id(),
            **kwargs,
        )
    )


def _delete_creditor_pin_info(creditor_id: int) -> None:
    PinInfo.query.filter_by(creditor_id=creditor_id).delete(
        synchronize_session=False
    )


def _stop_account_trade(
    creditor_id: int,
    debtor_id: int,
    object_update_id: int,
    current_ts: datetime,
) -> None:
    # NOTE: When an account has been deleted, notification messages must be
    # sent to the subsystem that performs automatic circular trades. These
    # are otherwise regular notifications, but they contain the default safe
    # values for all of the fields. (The default values forbid all automatic
    # circular trades for the account.)
    db.session.add(UpdatedLedgerSignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=object_update_id,
        account_id='',
        creation_date=DATE0,
        principal=0,
        last_transfer_number=0,
        ts=current_ts,
    ))
    db.session.add(UpdatedPolicySignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=object_update_id,
        policy_name=None,
        min_principal=MIN_INT64,
        max_principal=MAX_INT64,
        peg_exchange_rate=None,
        peg_debtor_id=None,
        ts=current_ts,
    ))
    db.session.add(UpdatedFlagsSignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=object_update_id,
        config_flags=DEFAULT_CONFIG_FLAGS,
        ts=current_ts,
    ))


def _delete_creditor_accounts(creditor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    object_update_id = db.session.scalar(uid_seq)
    accounts = (
        Account.query.filter_by(creditor_id=creditor_id)
        .with_for_update()
        .all()
    )
    for account in accounts:
        _stop_account_trade(
            creditor_id, account.debtor_id, object_update_id, current_ts
        )
        db.session.delete(account)


def _delete_creditor_running_transfers(creditor_id: int) -> None:
    RunningTransfer.query.filter_by(creditor_id=creditor_id).delete(
        synchronize_session=False
    )
