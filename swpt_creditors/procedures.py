from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, Tuple, List
from flask import current_app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import tuple_
from .extensions import db
from .models import AccountLedger, LedgerAddition, CommittedTransfer, PendingCommittedTransfer, \
    InitiatedTransfer, RunningTransfer, \
    MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID, date_to_int24

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_5_SECONDS = timedelta(seconds=10)
PENDING_COMMITTED_TRANSFER_PK = tuple_(
    PendingCommittedTransfer.debtor_id,
    PendingCommittedTransfer.creditor_id,
    PendingCommittedTransfer.transfer_seqnum,
)


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
    assert len(coordinator_type) <= 30
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
    current_ts = datetime.now(tz=timezone.utc)
    ledger_has_not_been_updated_soon = current_ts - ledger.last_update_ts > TD_5_SECONDS
    if transfer_epoch > ledger.epoch:
        # A new "epoch" has started -- the old ledger must be
        # discarded, and a brand new ledger created.
        ledger.epoch = transfer_epoch
        ledger.principal = 0
        ledger.next_transfer_seqnum = (date_to_int24(transfer_epoch) << 40) + 1
        ledger.last_update_ts = current_ts

    # If committed transfers come in the right order, it is faster to
    # update the account ledger right away. We must be careful,
    # though, not to update the account ledger too often, because this
    # can cause a row lock contention.
    if ledger.next_transfer_seqnum == transfer_seqnum and ledger_has_not_been_updated_soon:
        ledger.principal = new_account_principal
        ledger.next_transfer_seqnum += 1
        ledger.last_update_ts = current_ts
        db.session.add(LedgerAddition(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            transfer_seqnum=transfer_seqnum,
        ))
    else:
        # A dedicated asynchronous task will do the addition to the account
        # ledger later. (See `process_pending_committed_transfers()`.)
        db.session.add(PendingCommittedTransfer(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            transfer_seqnum=transfer_seqnum,
            committed_at_ts=committed_at_ts,
        ))


@atomic
def process_pending_committed_transfers(debtor_id: int, creditor_id: int, max_count: int = None) -> None:
    ledger = _get_or_create_ledger(debtor_id, creditor_id, lock=True)
    query = PendingCommittedTransfer.\
        query(PendingCommittedTransfer.transfer_seqnum).\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        order_by(PendingCommittedTransfer.transfer_seqnum)
    if max_count is not None:
        query = query.limit(max_count)
    current_ts = datetime.now(tz=timezone.utc)
    pks_to_delete = []
    for transfer_seqnum in [t[0] for t in query.all()]:
        if transfer_seqnum == ledger.next_transfer_seqnum:
            new_account_principal = CommittedTransfer.\
                query(CommittedTransfer.new_account_principal).\
                filter_by(debtor_id=debtor_id, creditor_id=creditor_id, transfer_seqnum=transfer_seqnum).\
                scalar()
            ledger.principal = new_account_principal
            ledger.next_transfer_seqnum += 1
            ledger.last_update_ts = current_ts
            db.session.add(LedgerAddition(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                transfer_seqnum=transfer_seqnum,
            ))
        elif transfer_seqnum > ledger.next_transfer_seqnum:
            break
        pks_to_delete.append((creditor_id, debtor_id, transfer_seqnum))
    PendingCommittedTransfer.filter(PENDING_COMMITTED_TRANSFER_PK.in_(pks_to_delete)).delete(synchronize_session=False)


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
