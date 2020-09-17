import re
from uuid import UUID
from math import floor
from datetime import datetime, timezone, date, timedelta
from typing import TypeVar, Callable, Optional, List
from sqlalchemy.orm import exc
from swpt_creditors.extensions import db
from swpt_creditors.models import AccountData, PendingLogEntry, RunningTransfer, \
    CommittedTransfer, PrepareTransferSignal, FinalizeTransferSignal, PendingLedgerUpdate, \
    SC_OK, SC_CANCELED_BY_THE_SENDER, SC_UNEXPECTED_ERROR, MAX_INT32, MIN_INT64, MAX_INT64, \
    TRANSFER_NOTE_MAX_BYTES, TRANSFER_NOTE_FORMAT_REGEX
from .common import get_paths_and_types
from .creditors import get_active_creditor
from . import errors

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

RE_TRANSFER_NOTE_FORMAT = re.compile(TRANSFER_NOTE_FORMAT_REGEX)


@atomic
def get_committed_transfer(
        creditor_id: int,
        debtor_id: int,
        creation_date: date,
        transfer_number: int) -> Optional[CommittedTransfer]:

    return CommittedTransfer.query.\
        filter_by(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            creation_date=creation_date,
            transfer_number=transfer_number,
        ).\
        one_or_none()


@atomic
def get_running_transfer(creditor_id: int, transfer_uuid: UUID, lock=False) -> Optional[RunningTransfer]:
    query = RunningTransfer.query.filter_by(creditor_id=creditor_id, transfer_uuid=transfer_uuid)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def initiate_running_transfer(
        creditor_id: int,
        transfer_uuid: UUID,
        debtor_id: int,
        amount: int,
        recipient_uri: str,
        recipient_id: str,
        transfer_note_format: str,
        transfer_note: str,
        *,
        deadline: datetime = None,
        min_interest_rate: float = -100.0,
        locked_amount: int = 0) -> RunningTransfer:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert 0 <= amount <= MAX_INT64
    assert min_interest_rate >= -100.0
    assert 0 <= locked_amount <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_active_creditor(creditor_id)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    transfer_data = {
        'debtor_id': debtor_id,
        'amount': amount,
        'recipient_uri': recipient_uri,
        'recipient_id': recipient_id,
        'transfer_note_format': transfer_note_format,
        'transfer_note': transfer_note,
        'deadline': deadline,
        'min_interest_rate': min_interest_rate,
        'locked_amount': locked_amount,
    }

    rt = get_running_transfer(creditor_id, transfer_uuid)
    if rt:
        if any(getattr(rt, attr) != value for attr, value in transfer_data.items()):
            raise errors.UpdateConflict()
        raise errors.TransferExists()

    new_running_transfer = RunningTransfer(
        creditor_id=creditor_id,
        transfer_uuid=transfer_uuid,
        latest_update_ts=current_ts,
        **transfer_data,
    )
    with db.retry_on_integrity_error():
        db.session.add(new_running_transfer)

    db.session.add(PrepareTransferSignal(
        creditor_id=creditor_id,
        coordinator_request_id=new_running_transfer.coordinator_request_id,
        debtor_id=debtor_id,
        recipient=recipient_id,
        locked_amount=locked_amount,
        min_interest_rate=min_interest_rate,
        max_commit_delay=_calc_max_commit_delay(current_ts, deadline),
        inserted_at_ts=current_ts,
    ))

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.transfer,
        object_uri=paths.transfer(creditorId=creditor_id, transferUuid=transfer_uuid),
        object_update_id=1,
    ))

    return new_running_transfer


@atomic
def cancel_running_transfer(creditor_id: int, transfer_uuid: UUID) -> RunningTransfer:
    rt = get_running_transfer(creditor_id, transfer_uuid, lock=True)
    if rt is None:
        raise errors.TransferDoesNotExist()

    if rt.is_settled:
        raise errors.ForbiddenTransferCancellation()

    _finalize_running_transfer(rt, error_code=SC_CANCELED_BY_THE_SENDER)
    return rt


@atomic
def delete_running_transfer(creditor_id: int, transfer_uuid: UUID) -> None:
    number_of_deleted_rows = RunningTransfer.query.\
        filter_by(creditor_id=creditor_id, transfer_uuid=transfer_uuid).\
        delete(synchronize_session=False)

    if number_of_deleted_rows == 0:
        raise errors.TransferDoesNotExist()
    assert number_of_deleted_rows == 1

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=datetime.now(tz=timezone.utc),
        object_type=types.transfer,
        object_uri=paths.transfer(creditorId=creditor_id, transferUuid=transfer_uuid),
        is_deleted=True,
    ))


@atomic
def get_creditor_transfer_uuids(creditor_id: int, count: int = 1, prev: UUID = None) -> List[UUID]:
    assert count >= 1
    assert prev is None or isinstance(prev, UUID)

    query = db.session.\
        query(RunningTransfer.transfer_uuid).\
        filter(RunningTransfer.creditor_id == creditor_id).\
        order_by(RunningTransfer.transfer_uuid)

    if prev is not None:
        query = query.filter(RunningTransfer.transfer_uuid > prev)

    return [t[0] for t in query.limit(count).all()]


@atomic
def process_account_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
        transfer_number: int,
        coordinator_type: str,
        sender: str,
        recipient: str,
        acquired_amount: int,
        transfer_note_format: str,
        transfer_note: str,
        committed_at_ts: datetime,
        principal: int,
        ts: datetime,
        previous_transfer_number: int,
        retention_interval: timedelta) -> None:

    assert 0 < transfer_number <= MAX_INT64
    assert coordinator_type == '' or len(coordinator_type) <= 30 and coordinator_type.encode('ascii')
    assert sender == '' or len(sender) <= 100 and coordinator_type.encode('ascii')
    assert recipient == '' or len(recipient) <= 100 and coordinator_type.encode('ascii')
    assert acquired_amount != 0
    assert RE_TRANSFER_NOTE_FORMAT.match(transfer_note_format)
    assert len(transfer_note) <= TRANSFER_NOTE_MAX_BYTES
    assert len(transfer_note.encode('utf8')) <= TRANSFER_NOTE_MAX_BYTES
    assert MIN_INT64 <= acquired_amount <= MAX_INT64
    assert MIN_INT64 <= principal <= MAX_INT64
    assert 0 <= previous_transfer_number <= MAX_INT64
    assert previous_transfer_number < transfer_number

    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - min(ts, committed_at_ts)) > retention_interval:
        return

    committed_transfer_query = CommittedTransfer.query.filter_by(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        transfer_number=transfer_number,
    )
    if db.session.query(committed_transfer_query.exists()).scalar():
        return

    # NOTE: We must obtain a "FOR SHARE" lock here to ensure that the
    # `ledger_last_transfer_number` will not be increased by another
    # concurrent transaction, without inserting a corresponding
    # `PendingLedgerUpdate` record, which would result in the ledger
    # not being updated.
    ledger_data_query = db.session.\
        query(AccountData.creation_date, AccountData.ledger_last_transfer_number).\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        with_for_update(read=True)
    try:
        ledger_date, ledger_last_transfer_number = ledger_data_query.one()
    except exc.NoResultFound:
        return

    with db.retry_on_integrity_error():
        db.session.add(CommittedTransfer(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            creation_date=creation_date,
            transfer_number=transfer_number,
            coordinator_type=coordinator_type,
            sender_id=sender,
            recipient_id=recipient,
            acquired_amount=acquired_amount,
            transfer_note_format=transfer_note_format,
            transfer_note=transfer_note,
            committed_at_ts=committed_at_ts,
            principal=principal,
            previous_transfer_number=previous_transfer_number,
        ))

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at_ts=current_ts,
        object_type=types.committed_transfer,
        object_uri=paths.committed_transfer(
            creditorId=creditor_id,
            debtorId=debtor_id,
            creationDate=creation_date,
            transferNumber=transfer_number,
        ),
    ))

    if creation_date == ledger_date and previous_transfer_number == ledger_last_transfer_number:
        ensure_pending_ledger_update(creditor_id, debtor_id)


@atomic
def ensure_pending_ledger_update(creditor_id: int, debtor_id: int) -> None:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    pending_ledger_update_query = PendingLedgerUpdate.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    if not db.session.query(pending_ledger_update_query.exists()).scalar():
        with db.retry_on_integrity_error():
            db.session.add(PendingLedgerUpdate(creditor_id=creditor_id, debtor_id=debtor_id))


@atomic
def process_rejected_direct_transfer_signal(
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        debtor_id: int,
        creditor_id: int) -> None:

    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt and not rt.is_finalized:
        if status_code != SC_OK and rt.debtor_id == debtor_id and rt.creditor_id == creditor_id:
            _finalize_running_transfer(rt, error_code=status_code, total_locked_amount=total_locked_amount)
        else:
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_prepared_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        recipient: str) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 <= locked_amount <= MAX_INT64

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == creditor_id
        and rt.recipient_id == recipient
    )
    if rt_matches_the_signal:
        assert rt is not None
        if not rt.is_finalized and rt.transfer_id is None:
            rt.transfer_id = transfer_id

        if rt.transfer_id == transfer_id:
            db.session.add(FinalizeTransferSignal(
                creditor_id=creditor_id,
                debtor_id=rt.debtor_id,
                transfer_id=transfer_id,
                coordinator_id=coordinator_id,
                coordinator_request_id=coordinator_request_id,
                committed_amount=rt.amount,
                transfer_note_format=rt.transfer_note_format,
                transfer_note=rt.transfer_note,
            ))
            return

    # The newly prepared transfer is dismissed.
    db.session.add(FinalizeTransferSignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=0,
        transfer_note_format='',
        transfer_note='',
    ))


@atomic
def process_finalized_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        recipient: str,
        status_code: str,
        total_locked_amount: int) -> None:

    assert 0 <= len(status_code.encode('ascii')) <= 30
    assert 0 <= total_locked_amount <= MAX_INT64

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == creditor_id
        and rt.transfer_id == transfer_id
    )
    if rt_matches_the_signal:
        assert rt is not None
        if status_code == SC_OK and committed_amount == rt.amount and recipient == rt.recipient_id:
            _finalize_running_transfer(rt)
        elif status_code != SC_OK and committed_amount == 0 and recipient == rt.recipient_id:
            _finalize_running_transfer(rt, error_code=status_code, total_locked_amount=total_locked_amount)
        else:
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


def _find_running_transfer(coordinator_id: int, coordinator_request_id: int) -> Optional[RunningTransfer]:
    return RunningTransfer.query.\
        filter_by(creditor_id=coordinator_id, coordinator_request_id=coordinator_request_id).\
        one_or_none()


def _finalize_running_transfer(rt: RunningTransfer, error_code: str = None, total_locked_amount: int = None) -> None:
    if not rt.is_finalized:
        current_ts = datetime.now(tz=timezone.utc)
        rt.latest_update_id += 1
        rt.latest_update_ts = current_ts
        rt.finalized_at_ts = current_ts
        rt.error_code = error_code
        rt.total_locked_amount = total_locked_amount

        paths, types = get_paths_and_types()
        db.session.add(PendingLogEntry(
            creditor_id=rt.creditor_id,
            added_at_ts=current_ts,
            object_type=types.transfer,
            object_uri=paths.transfer(creditorId=rt.creditor_id, transferUuid=rt.transfer_uuid),
            object_update_id=rt.latest_update_id,
            data_finalized_at_ts=current_ts,
            data_error_code=error_code,
        ))


def _calc_max_commit_delay(current_ts: datetime, deadline: datetime = None) -> int:
    seconds = floor((deadline - current_ts).total_seconds()) if deadline else MAX_INT32
    return max(0, min(seconds, MAX_INT32))
