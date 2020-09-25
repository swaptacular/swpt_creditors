from typing import TypeVar, Callable, List, Tuple, Optional, Iterable
from random import randint
from datetime import datetime
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

ACTIVATION_STATUS_MASK = Creditor.STATUS_IS_ACTIVATED_FLAG | Creditor.STATUS_IS_DEACTIVATED_FLAG


@atomic
def generate_new_creditor_id() -> int:
    agent_config = AgentConfig.query.one()
    return randint(agent_config.min_creditor_id, agent_config.max_creditor_id)


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
def get_creditor_ids(start_from: int, count: int = 1) -> Tuple[List[int], Optional[int]]:
    query = db.session.\
        query(Creditor.creditor_id).\
        filter(Creditor.creditor_id >= start_from).\
        filter(Creditor.status.op('&')(ACTIVATION_STATUS_MASK) == Creditor.STATUS_IS_ACTIVATED_FLAG).\
        order_by(Creditor.creditor_id).\
        limit(count)
    creditor_ids = [t[0] for t in query.all()]

    if len(creditor_ids) > 0:
        next_creditor_id = creditor_ids[-1] + 1
    else:
        next_creditor_id = AgentConfig.query.one().max_creditor_id + 1

    if next_creditor_id > MAX_INT64 or next_creditor_id <= start_from:
        next_creditor_id = None

    return creditor_ids, next_creditor_id


@atomic
def reserve_creditor(creditor_id, verify_correctness=True) -> Creditor:
    if verify_correctness and not _is_correct_creditor_id(creditor_id):
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

    creditor_can_be_activated = creditor and (creditor.is_activated or reservation_id == creditor.reservation_id)
    if not creditor_can_be_activated:
        raise errors.InvalidReservationId()

    assert creditor is not None
    creditor.activate()

    return creditor


@atomic
def deactivate_creditor(creditor_id: int) -> None:
    creditor = get_active_creditor(creditor_id, lock=True)
    if creditor:
        creditor.deactivate()
        _delete_creditor_accounts(creditor_id)
        _delete_creditor_running_transfers(creditor_id)


@atomic
def get_active_creditor(creditor_id: int, lock: bool = False) -> Optional[Creditor]:
    creditor = _get_creditor(creditor_id, lock=lock)
    if creditor and creditor.is_activated and not creditor.is_deactivated:
        return creditor


@atomic
def get_log_entries(creditor_id: int, *, count: int = 1, prev: int = 0) -> Tuple[List[LogEntry], int]:
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
    query = db.session.query(PendingLogEntry.creditor_id).distinct()
    return [t[0] for t in query.all()]


@atomic
def process_pending_log_entries(creditor_id: int) -> None:
    creditor = _get_creditor(creditor_id, lock=True)
    if creditor:
        pending_log_entries = PendingLogEntry.query.\
            filter_by(creditor_id=creditor_id).\
            order_by(PendingLogEntry.pending_entry_id).\
            with_for_update().\
            all()

        for entry in pending_log_entries:
            _process_pending_log_entry(creditor, entry)


def _process_pending_log_entry(creditor: Creditor, entry: PendingLogEntry) -> None:
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

    if entry.get_object_type(types) == types.transfer and (entry.is_created or entry.is_deleted):
        # NOTE: When a running transfer has been created or deleted,
        # the client should be informed about the update in his list
        # of transfers. The write to the log is performed now, because
        # at the time the running transfer was created/deleted, the
        # correct value of the `object_update_id` field had been
        # unknown (a lock on creditor's table row would be required).
        _add_transfers_list_update_log_entry(creditor, entry.added_at)

    db.session.delete(entry)


def _add_transfers_list_update_log_entry(creditor: Creditor, added_at: datetime) -> None:
    paths, types = get_paths_and_types()
    creditor.transfers_list_latest_update_id += 1
    creditor.transfers_list_latest_update_ts = added_at

    _add_log_entry(
        creditor,
        object_type=types.transfers_list,
        object_uri=paths.transfers_list(creditorId=creditor.creditor_id),
        object_update_id=creditor.transfers_list_latest_update_id,
        added_at=creditor.transfers_list_latest_update_ts,
    )


def _get_creditor(creditor_id: int, lock=False) -> Optional[Creditor]:
    query = Creditor.query.filter_by(creditor_id=creditor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


def _add_log_entry(creditor: Creditor, **kwargs) -> None:
    db.session.add(LogEntry(
        creditor_id=creditor.creditor_id,
        entry_id=creditor.generate_log_entry_id(),
        **kwargs,
    ))


def _delete_creditor_accounts(creditor_id: int) -> None:
    Account.query.\
        filter_by(creditor_id=creditor_id).\
        delete(synchronize_session=False)


def _delete_creditor_running_transfers(creditor_id: int) -> None:
    RunningTransfer.query.\
        filter_by(creditor_id=creditor_id).\
        delete(synchronize_session=False)


def _is_correct_creditor_id(creditor_id: int) -> bool:
    try:
        config = AgentConfig.query.one()
        if not config.min_creditor_id <= creditor_id <= config.max_creditor_id:
            raise ValueError()
    except (exc.NoResultFound, ValueError):
        return False

    return True
