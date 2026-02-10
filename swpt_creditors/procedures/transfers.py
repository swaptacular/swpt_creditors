from uuid import UUID
from math import floor
from datetime import datetime, timezone, date, timedelta
from typing import TypeVar, Callable, Optional, List
from sqlalchemy import select
from sqlalchemy.orm import exc, defer
from sqlalchemy.dialects import postgresql
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    AccountData,
    LogEntry,
    PendingLogEntry,
    RunningTransfer,
    CommittedTransfer,
    PrepareTransferSignal,
    FinalizeTransferSignal,
    PendingLedgerUpdate,
    RejectedConfigSignal,
    SC_OK,
    SC_CANCELED_BY_THE_SENDER,
    SC_UNEXPECTED_ERROR,
    MAX_INT32,
    T_INFINITY,
)
from .creditors import get_active_creditor
from . import errors

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

DEFER_RUNNING_TRANSFER_TOASTED_COLUMNS = [defer(RunningTransfer.transfer_note)]
ENSURE_PENDING_LEDGER_UPDATE_STATEMENT = postgresql.insert(
    PendingLedgerUpdate.__table__
).on_conflict_do_nothing()


@atomic
def get_committed_transfer(
    creditor_id: int, debtor_id: int, creation_date: date, transfer_number: int
) -> Optional[CommittedTransfer]:
    return CommittedTransfer.query.filter_by(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        transfer_number=transfer_number,
    ).one_or_none()


@atomic
def get_running_transfer(
    creditor_id: int, transfer_uuid: UUID, lock=False
) -> Optional[RunningTransfer]:
    query = RunningTransfer.query.filter_by(
        creditor_id=creditor_id, transfer_uuid=transfer_uuid
    )
    if lock:
        query = query.with_for_update(key_share=True)

    return query.one_or_none()


@atomic
def initiate_running_transfer(
    *,
    creditor_id: int,
    transfer_uuid: UUID,
    debtor_id: int,
    amount: int,
    recipient_uri: str,
    recipient: str,
    transfer_note_format: str,
    transfer_note: str,
    deadline: datetime = None,
    final_interest_rate_ts: datetime = T_INFINITY,
    locked_amount: int = 0
) -> RunningTransfer:
    current_ts = datetime.now(tz=timezone.utc)

    creditor = get_active_creditor(creditor_id)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    transfer_data = {
        "debtor_id": debtor_id,
        "amount": amount,
        "recipient_uri": recipient_uri,
        "recipient": recipient,
        "transfer_note_format": transfer_note_format,
        "transfer_note": transfer_note,
        "deadline": deadline,
        "final_interest_rate_ts": final_interest_rate_ts,
        "locked_amount": locked_amount,
    }

    rt = get_running_transfer(creditor_id, transfer_uuid)
    if rt:
        if any(
            getattr(rt, attr) != value for attr, value in transfer_data.items()
        ):
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

    # NOTE: The log entry that informs about the change in the
    # creditor's `TransfersList` will be added later, when this
    # pending log entry is processed.
    db.session.add(
        PendingLogEntry(
            creditor_id=creditor_id,
            added_at=current_ts,
            object_type_hint=LogEntry.OTH_TRANSFER,
            transfer_uuid=transfer_uuid,
            object_update_id=1,
        )
    )

    db.session.add(
        PrepareTransferSignal(
            creditor_id=creditor_id,
            coordinator_request_id=new_running_transfer.coordinator_request_id,
            debtor_id=debtor_id,
            recipient=recipient,
            locked_amount=locked_amount,
            final_interest_rate_ts=final_interest_rate_ts,
            max_commit_delay=_calc_max_commit_delay(current_ts, deadline),
            inserted_at=current_ts,
        )
    )

    return new_running_transfer


@atomic
def cancel_running_transfer(
    creditor_id: int, transfer_uuid: UUID
) -> RunningTransfer:
    rt = get_running_transfer(creditor_id, transfer_uuid, lock=True)
    if rt is None:
        raise errors.TransferDoesNotExist()

    if rt.is_settled:
        raise errors.ForbiddenTransferCancellation()

    _finalize_running_transfer(rt, error_code=SC_CANCELED_BY_THE_SENDER)
    return rt


@atomic
def delete_running_transfer(creditor_id: int, transfer_uuid: UUID) -> None:
    number_of_deleted_rows = RunningTransfer.query.filter_by(
        creditor_id=creditor_id, transfer_uuid=transfer_uuid
    ).delete(synchronize_session=False)

    if number_of_deleted_rows == 0:
        raise errors.TransferDoesNotExist()

    assert number_of_deleted_rows == 1

    # NOTE: The log entry that informs about the change in the
    # creditor's `TransfersList` will be added later, when this
    # pending log entry is processed.
    db.session.add(
        PendingLogEntry(
            creditor_id=creditor_id,
            added_at=datetime.now(tz=timezone.utc),
            object_type_hint=LogEntry.OTH_TRANSFER,
            transfer_uuid=transfer_uuid,
            is_deleted=True,
        )
    )


@atomic
def get_creditor_transfer_uuids(
    creditor_id: int, *, count: int = 1, prev: UUID = None
) -> List[UUID]:
    query = (
        select(RunningTransfer.transfer_uuid)
        .where(RunningTransfer.creditor_id == creditor_id)
        .order_by(RunningTransfer.transfer_uuid)
        .limit(count)
    )
    if prev is not None:
        query = query.where(RunningTransfer.transfer_uuid > prev)

    return db.session.execute(query).scalars().all()


@atomic
def ensure_pending_ledger_update(creditor_id: int, debtor_id: int) -> None:
    db.session.execute(
        ENSURE_PENDING_LEDGER_UPDATE_STATEMENT.values(
            creditor_id=creditor_id, debtor_id=debtor_id
        )
    )


@atomic
def process_account_transfer_signal(
    *,
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
    committed_at: datetime,
    principal: int,
    ts: datetime,
    previous_transfer_number: int,
    retention_interval: timedelta
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - min(ts, committed_at)) > retention_interval:
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
    ledger_data = db.session.execute(
        select(
            AccountData.creation_date,
            AccountData.ledger_last_transfer_number,
        )
        .where(
            AccountData.creditor_id == creditor_id,
            AccountData.debtor_id == debtor_id,
        )
        .with_for_update(read=True)
    )
    try:
        ledger_date, ledger_last_transfer_number = ledger_data.one()
    except exc.NoResultFound:
        return

    with db.retry_on_integrity_error():
        db.session.add(
            CommittedTransfer(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                creation_date=creation_date,
                transfer_number=transfer_number,
                coordinator_type=coordinator_type,
                sender=sender,
                recipient=recipient,
                acquired_amount=acquired_amount,
                transfer_note_format=transfer_note_format,
                transfer_note=transfer_note,
                committed_at=committed_at,
                principal=principal,
                previous_transfer_number=previous_transfer_number,
            )
        )

    db.session.add(
        PendingLogEntry(
            creditor_id=creditor_id,
            added_at=current_ts,
            object_type_hint=LogEntry.OTH_COMMITTED_TRANSFER,
            debtor_id=debtor_id,
            creation_date=creation_date,
            transfer_number=transfer_number,
        )
    )

    if (
        creation_date == ledger_date
        and previous_transfer_number == ledger_last_transfer_number
    ):
        ensure_pending_ledger_update(creditor_id, debtor_id)


@atomic
def process_rejected_direct_transfer_signal(
    *,
    coordinator_id: int,
    coordinator_request_id: int,
    status_code: str,
    total_locked_amount: int,
    debtor_id: int,
    creditor_id: int
) -> None:
    rt = _find_running_transfer(
        coordinator_id, coordinator_request_id, defer_toasted=True
    )
    if rt and not rt.is_finalized:
        if (
            status_code != SC_OK
            and rt.debtor_id == debtor_id
            and rt.creditor_id == creditor_id
        ):
            _finalize_running_transfer(
                rt,
                error_code=status_code,
                total_locked_amount=total_locked_amount,
            )
        else:
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_prepared_direct_transfer_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_id: int,
    coordinator_request_id: int,
    locked_amount: int,
    recipient: str
) -> None:
    def dismiss_prepared_transfer():
        db.session.add(
            FinalizeTransferSignal(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                transfer_id=transfer_id,
                coordinator_id=coordinator_id,
                coordinator_request_id=coordinator_request_id,
                committed_amount=0,
                transfer_note_format="",
                transfer_note="",
            )
        )

    rt = _find_running_transfer(coordinator_id, coordinator_request_id)

    the_signal_matches_the_transfer = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == creditor_id
        and rt.recipient == recipient
        and rt.locked_amount <= locked_amount
    )
    if the_signal_matches_the_transfer:
        assert rt is not None

        if not rt.is_finalized and rt.transfer_id is None:
            rt.transfer_id = transfer_id

        if rt.transfer_id == transfer_id:
            db.session.add(
                FinalizeTransferSignal(
                    creditor_id=creditor_id,
                    debtor_id=rt.debtor_id,
                    transfer_id=transfer_id,
                    coordinator_id=coordinator_id,
                    coordinator_request_id=coordinator_request_id,
                    committed_amount=rt.amount,
                    transfer_note_format=rt.transfer_note_format,
                    transfer_note=rt.transfer_note,
                )
            )
            return

    dismiss_prepared_transfer()


@atomic
def process_finalized_direct_transfer_signal(
    *,
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_id: int,
    coordinator_request_id: int,
    committed_amount: int,
    status_code: str,
    total_locked_amount: int
) -> None:
    rt = _find_running_transfer(
        coordinator_id, coordinator_request_id, defer_toasted=True
    )
    the_signal_matches_the_transfer = (
        rt is not None
        and rt.debtor_id == debtor_id
        and rt.creditor_id == creditor_id
        and rt.transfer_id == transfer_id
    )
    if the_signal_matches_the_transfer:
        assert rt is not None

        if status_code == SC_OK and committed_amount == rt.amount:
            _finalize_running_transfer(rt)
        elif status_code != SC_OK and committed_amount == 0:
            _finalize_running_transfer(
                rt,
                error_code=status_code,
                total_locked_amount=total_locked_amount,
            )
        else:
            _finalize_running_transfer(rt, error_code=SC_UNEXPECTED_ERROR)


@atomic
def process_configure_account_signal(
    debtor_id: int,
    creditor_id: int,
    ts: datetime,
    seqnum: int,
    negligible_amount: float,
    config_flags: int,
    config_data: str,
) -> None:
    db.session.add(
        RejectedConfigSignal(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            config_ts=ts,
            config_seqnum=seqnum,
            config_flags=config_flags,
            config_data=config_data,
            negligible_amount=negligible_amount,
            rejection_code='NO_CONNECTION_TO_DEBTOR',
        )
    )


def _find_running_transfer(
    coordinator_id: int,
    coordinator_request_id: int,
    defer_toasted: bool = False,
) -> Optional[RunningTransfer]:
    query = RunningTransfer.query.filter_by(
        creditor_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
    )
    if defer_toasted:
        query = query.options(*DEFER_RUNNING_TRANSFER_TOASTED_COLUMNS)

    return query.one_or_none()


def _finalize_running_transfer(
    rt: RunningTransfer,
    error_code: str = None,
    total_locked_amount: int = None,
) -> None:
    if not rt.is_finalized:
        current_ts = datetime.now(tz=timezone.utc)

        rt.latest_update_id += 1
        rt.latest_update_ts = current_ts
        rt.finalized_at = current_ts
        rt.error_code = error_code
        rt.total_locked_amount = total_locked_amount

        db.session.add(
            PendingLogEntry(
                creditor_id=rt.creditor_id,
                added_at=current_ts,
                object_type_hint=LogEntry.OTH_TRANSFER,
                transfer_uuid=rt.transfer_uuid,
                object_update_id=rt.latest_update_id,
                data_finalized_at=current_ts,
                data_error_code=error_code,
            )
        )


def _calc_max_commit_delay(
    current_ts: datetime, deadline: datetime = None
) -> int:
    seconds = (
        floor((deadline - current_ts).total_seconds())
        if deadline
        else MAX_INT32
    )
    return max(0, min(seconds, MAX_INT32))
