from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, Tuple, List
from flask import current_app
from sqlalchemy.exc import IntegrityError
from .extensions import db
from .models import AccountLedger, LedgerAddition, CommittedTransfer, PendingCommittedTransfer, \
    InitiatedTransfer, RunningTransfer, \
    MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID, date_to_int24

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic


@atomic
def process_committed_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_seqnum: int,
        transfer_epoch: date,
        coordinator_type: str,
        other_creditor_id: int,
        committed_at_ts: datetime,
        committed_amount: int,
        transfer_info: dict,
        new_account_principal: int) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= other_creditor_id <= MAX_INT64
    assert 0 < transfer_seqnum <= MAX_INT64
    assert committed_amount != 0
    assert -MAX_INT64 <= committed_amount <= MAX_INT64
    assert -MAX_INT64 <= new_account_principal <= MAX_INT64

    db.session.add(CommittedTransfer(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_seqnum=transfer_seqnum,
        transfer_epoch=transfer_epoch,
        coordinator_type=coordinator_type,
        other_creditor_id=other_creditor_id,
        committed_at_ts=committed_at_ts,
        committed_amount=committed_amount,
        transfer_info=transfer_info,
        new_account_principal=new_account_principal,
    ))
    try:
        db.session.flush()
    except IntegrityError:
        # Normally, this can happen only when the committed transfer
        # message has been re-delivered. Therefore, no action should
        # be taken.
        db.session.rollback()
        return

    ledger = _get_or_create_ledger(debtor_id, creditor_id)
    if transfer_epoch > ledger.epoch:
        # A new "epoch" has started -- the old ledger must be
        # discarded, and a brand new ledger created.
        ledger.epoch = transfer_epoch
        ledger.principal = 0
        ledger.first_transfer_seqnum = (date_to_int24(transfer_epoch) << 40) + 1
        ledger.next_transfer_seqnum = ledger.first_transfer_seqnum

    db.session.add(PendingCommittedTransfer(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_seqnum=transfer_seqnum,
        committed_at_ts=committed_at_ts,
    ))


def _create_ledger(debtor_id: int, creditor_id: int) -> AccountLedger:
    ledger = AccountLedger(debtor_id=debtor_id, creditor_id=creditor_id)
    with db.retry_on_integrity_error():
        db.session.add(ledger)
    return ledger


def _get_or_create_ledger(debtor_id: int, creditor_id: int, lock: bool = False) -> AccountLedger:
    if lock:
        ledger = AccountLedger.lock_instance((debtor_id, creditor_id))
    else:
        ledger = AccountLedger.get_instance((debtor_id, creditor_id))
    return ledger or _create_ledger(debtor_id, creditor_id)
