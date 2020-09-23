from typing import TypeVar, Callable, List, Tuple, Optional, Iterable
from datetime import datetime, timezone
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import exc
from swpt_creditors.extensions import db
from swpt_creditors.models import AgentConfig, Creditor, LogEntry, PendingLogEntry, Account, \
    RunningTransfer, MIN_INT64, MAX_INT64
from .common import get_paths_and_types
from . import errors

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


@atomic
def configure_agent(*, min_creditor_id: int, max_creditor_id: int) -> None:
    assert MIN_INT64 <= min_creditor_id <= MAX_INT64
    assert MIN_INT64 <= max_creditor_id <= MAX_INT64
    assert min_creditor_id <= max_creditor_id

    agent_config = AgentConfig.query.with_for_update().one_or_none()
    if agent_config:
        agent_config.min_creditor_id = min_creditor_id
        agent_config.max_creditor_id = max_creditor_id
    else:  # pragma: no cover
        with db.retry_on_integrity_error():
            db.session.add(AgentConfig(
                min_creditor_id=min_creditor_id,
                max_creditor_id=max_creditor_id,
            ))


@atomic
def reserve_creditor(creditor_id) -> Creditor:
    if not _is_correct_creditor_id(creditor_id):
        raise errors.InvalidCreditor()

    creditor = Creditor(creditor_id=creditor_id)
    db.session.add(creditor)
    try:
        db.session.flush()
    except IntegrityError:
        raise errors.CreditorExists() from None

    relic_log_entry_id = db.session.\
        query(func.max(LogEntry.entry_id)).\
        filter_by(creditor_id=creditor_id).\
        scalar()
    creditor.last_log_entry_id = 0 if relic_log_entry_id is None else relic_log_entry_id + 1  # a leap

    return creditor


@atomic
def activate_creditor(creditor_id: int, reservation_id: int) -> Creditor:
    creditor = _get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise errors.InvalidReservationId()

    if not creditor.is_activated:
        if reservation_id != creditor.reservation_id:
            raise errors.InvalidReservationId()
        creditor.activate()

    return creditor


@atomic
def deactivate_creditor(creditor_id: int) -> None:
    creditor = get_active_creditor(creditor_id, lock=True)
    if creditor:
        assert creditor.is_activated
        assert creditor.deactivated_at_date is None
        creditor.deactivated_at_date = datetime.now(tz=timezone.utc).date()
        Account.query.filter_by(creditor_id=creditor_id).delete(synchronize_session=False)
        RunningTransfer.query.filter_by(creditor_id=creditor_id).delete(synchronize_session=False)


@atomic
def get_active_creditor(creditor_id: int, lock: bool = False) -> Optional[Creditor]:
    creditor = _get_creditor(creditor_id, lock=lock)
    if creditor and creditor.is_activated and creditor.deactivated_at_date is None:
        return creditor


@atomic
def get_log_entries(creditor_id: int, *, count: int = 1, prev: int = 0) -> Tuple[List[LogEntry], int]:
    assert count >= 1
    assert 0 <= prev <= MAX_INT64

    last_log_entry_id = db.session.\
        query(Creditor.last_log_entry_id).\
        filter(Creditor.creditor_id == creditor_id).\
        scalar()

    if last_log_entry_id is None:
        raise errors.CreditorDoesNotExist()

    log_entries = LogEntry.query.\
        filter(LogEntry.creditor_id == creditor_id).\
        filter(LogEntry.entry_id > prev).\
        order_by(LogEntry.entry_id).\
        limit(count).\
        all()

    return log_entries, last_log_entry_id


@atomic
def get_creditors_with_pending_log_entries() -> Iterable[int]:
    return [t[0] for t in db.session.query(PendingLogEntry.creditor_id).distinct().all()]


@atomic
def process_pending_log_entries(creditor_id: int) -> None:
    creditor = _get_creditor(creditor_id, lock=True)
    if creditor is None:
        return

    pending_log_entries = PendingLogEntry.query.\
        filter_by(creditor_id=creditor_id).\
        order_by(PendingLogEntry.pending_entry_id).\
        with_for_update().\
        all()

    if pending_log_entries:
        paths, types = get_paths_and_types()
        for entry in pending_log_entries:
            aux_fields = {attr: getattr(entry, attr) for attr in LogEntry.AUX_FIELDS}
            data_fields = {attr: getattr(entry, attr) for attr in LogEntry.DATA_FIELDS}
            db.session.add(LogEntry(
                creditor_id=creditor_id,
                entry_id=creditor.generate_log_entry_id(),
                object_type=entry.object_type,
                object_uri=entry.object_uri,
                object_update_id=entry.object_update_id,
                added_at_ts=entry.added_at_ts,
                is_deleted=entry.is_deleted,
                data=entry.data,
                **aux_fields,
                **data_fields,
            ))

            if entry.get_object_type(types) == types.transfer and (entry.is_created or entry.is_deleted):
                # NOTE: When a running transfer has been created or
                # deleted, the client should be informed about the
                # update in his list of transfers. The actual write to
                # the log must be performed now, because at the time
                # the running transfer was created/deleted, the
                # correct value of the `object_update_id` field had
                # been unknown.
                _add_transfers_list_update_log_entry(creditor, entry.added_at_ts)

            db.session.delete(entry)


def _get_creditor(creditor_id: int, lock=False) -> Optional[Creditor]:
    query = Creditor.query.filter_by(creditor_id=creditor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


def _add_log_entry(
        creditor: Creditor,
        *,
        added_at_ts: datetime,
        object_type: str,
        object_uri: str,
        object_update_id: int = None,
        is_deleted: bool = None,
        data: dict = None) -> None:

    db.session.add(LogEntry(
        creditor_id=creditor.creditor_id,
        entry_id=creditor.generate_log_entry_id(),
        object_type=object_type,
        object_uri=object_uri,
        object_update_id=object_update_id,
        added_at_ts=added_at_ts,
        is_deleted=is_deleted,
        data=data,
    ))


def _add_transfers_list_update_log_entry(creditor: Creditor, added_at_ts: datetime) -> None:
    paths, types = get_paths_and_types()
    creditor.transfers_list_latest_update_id += 1
    creditor.transfers_list_latest_update_ts = added_at_ts

    _add_log_entry(
        creditor,
        object_type=types.transfers_list,
        object_uri=paths.transfers_list(creditorId=creditor.creditor_id),
        object_update_id=creditor.transfers_list_latest_update_id,
        added_at_ts=creditor.transfers_list_latest_update_ts,
    )


def _is_correct_creditor_id(creditor_id: int) -> bool:
    try:
        config = AgentConfig.query.one()
        if not config.min_creditor_id <= creditor_id <= config.max_creditor_id:
            raise ValueError()
    except (exc.NoResultFound, ValueError):
        return False

    return True
