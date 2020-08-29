from uuid import UUID
from datetime import datetime, timezone, date, timedelta
from typing import TypeVar, Callable, Optional
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import exc
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    AccountData, PendingLogEntry, DirectTransfer, RunningTransfer, CommittedTransfer,
    PrepareTransferSignal, FinalizeTransferSignal, MIN_INT64, MAX_INT64,
)
from .common import get_paths_and_types
from .accounts import ensure_pending_ledger_update
from .creditors import get_creditor
from . import errors

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


@atomic
def initiate_transfer(
        creditor_id: int,
        transfer_uuid: UUID,
        debtor_id: int,
        amount: int,
        recipient: str,
        note: dict,
        min_interest_rate: float,
        deadline: Optional[datetime]) -> DirectTransfer:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert 0 < amount <= MAX_INT64
    assert min_interest_rate >= 100.0
    assert type(note) is dict

    current_ts = datetime.now(tz=timezone.utc)
    creditor = get_creditor(creditor_id)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    new_transfer = DirectTransfer(
        creditor_id=creditor_id,
        transfer_uuid=transfer_uuid,
        debtor_id=debtor_id,
        amount=amount,
        recipient=recipient,
        note=note,
        deadline=deadline,
        min_interest_rate=min_interest_rate,
        latest_update_ts=current_ts,
    )
    _raise_error_if_transfer_exists(new_transfer)
    _insert_running_transfer_or_raise_update_conflict(new_transfer)

    with db.retry_on_integrity_error():
        db.session.add(new_transfer)

    return new_transfer


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
        transfer_note: str,
        committed_at_ts: datetime,
        principal: int,
        ts: datetime,
        previous_transfer_number: int,
        retention_interval: timedelta) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert 0 < transfer_number <= MAX_INT64
    assert acquired_amount != 0
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
def process_rejected_direct_transfer_signal(
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        debtor_id: int,
        creditor_id: int) -> None:

    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    if rt and not rt.is_finalized:
        if rt.debtor_id == debtor_id and rt.creditor_id == creditor_id:
            error_code = status_code
            total_locked_amount = total_locked_amount
        else:  # pragma:  no cover
            error_code = 'UNEXPECTED_ERROR'
            total_locked_amount = None

        _finalize_direct_transfer(
            rt.debtor_id,
            rt.transfer_uuid,
            error_code=error_code,
            total_locked_amount=total_locked_amount,
        )
        db.session.delete(rt)


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
    assert 0 < locked_amount <= MAX_INT64

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == creditor_id
        and rt.recipient == recipient
        and rt.amount <= locked_amount
    )
    if rt_matches_the_signal:
        assert rt is not None
        if not rt.is_finalized:
            rt.transfer_id = transfer_id

        if rt.transfer_id == transfer_id:
            db.session.add(FinalizeTransferSignal(
                creditor_id=creditor_id,
                debtor_id=rt.debtor_id,
                transfer_id=transfer_id,
                coordinator_id=coordinator_id,
                coordinator_request_id=coordinator_request_id,
                committed_amount=rt.amount,
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

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert 0 <= committed_amount <= MAX_INT64
    assert 0 <= len(status_code.encode('ascii')) <= 30

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)
    rt_matches_the_signal = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == creditor_id
        and rt.transfer_id == transfer_id
    )
    if rt_matches_the_signal:
        assert rt is not None
        if committed_amount == rt.amount and recipient == rt.recipient:
            error_code = None
            total_locked_amount = None
        elif committed_amount == 0 and recipient == rt.recipient:
            error_code = status_code
            total_locked_amount = total_locked_amount
        else:
            error_code = 'UNEXPECTED_ERROR'
            total_locked_amount = None

        _finalize_direct_transfer(
            rt.debtor_id,
            rt.transfer_uuid,
            error_code=error_code,
            total_locked_amount=total_locked_amount,
        )
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


def _find_running_transfer(coordinator_id: int, coordinator_request_id: int) -> Optional[RunningTransfer]:
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64

    return RunningTransfer.query.\
        filter_by(creditor_id=coordinator_id, coordinator_request_id=coordinator_request_id).\
        with_for_update().\
        one_or_none()


def _finalize_direct_transfer(
        debtor_id: int,
        transfer_uuid: int,
        *,
        finalized_at_ts: datetime = None,
        error_code: str = None,
        total_locked_amount: int = None) -> None:

    assert total_locked_amount is None or error_code is not None
    direct_transfer = DirectTransfer.lock_instance((debtor_id, transfer_uuid))
    if direct_transfer and direct_transfer.finalized_at_ts is None:
        direct_transfer.finalized_at_ts = finalized_at_ts or datetime.now(tz=timezone.utc)
        direct_transfer.error_code = error_code
        direct_transfer.total_locked_amount = total_locked_amount
        direct_transfer.latest_update_id += 1
        direct_transfer.latest_update_ts = direct_transfer.finalized_at_ts

        # TODO: Write to the log.


def _raise_error_if_transfer_exists(t1: DirectTransfer) -> None:
    t2 = DirectTransfer.get_instance((t1.creditor_id, t1.transfer_uuid))
    if t2:
        attributes = ['debtor_id', 'amount', 'recipient', 'note', 'deadline', 'min_interest_rate']
        if all(getattr(t1, name) == getattr(t2, name) for name in attributes):
            raise errors.TransferExists()

        raise errors.UpdateConflict()


def _insert_running_transfer_or_raise_update_conflict(t: DirectTransfer) -> None:
    running_transfer = RunningTransfer(
        creditor_id=t.creditor_id,
        transfer_uuid=t.transfer_uuid,
        debtor_id=t.debtor_id,
        recipient=t.recipient,
        amount=t.amount,
        transfer_note=str(t.note),  # TODO: this is wrong!
    )

    db.session.add(running_transfer)
    try:
        db.session.flush()
    except IntegrityError:
        raise errors.UpdateConflict()

    db.session.add(PrepareTransferSignal(
        creditor_id=t.creditor_id,
        debtor_id=t.debtor_id,
        coordinator_request_id=running_transfer.coordinator_request_id,
        min_locked_amount=t.amount,
        max_locked_amount=t.amount,
        recipient=t.recipient,
        min_interest_rate=t.min_interest_rate,
        max_commit_delay=t.max_commit_delay,
    ))

    return running_transfer
