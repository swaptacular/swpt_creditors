from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, Tuple, List
from flask import current_app
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import tuple_
from swpt_lib.utils import date_to_int24
from .extensions import db
from .models import AccountLedger, LedgerAddition, CommittedTransfer, PendingCommittedTransfer, \
    InitiatedTransfer, RunningTransfer, \
    MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID

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
        coordinator_type: str,
        other_creditor_id: int,
        committed_at_ts: datetime,
        committed_amount: int,
        transfer_info: dict,
        account_creation_date: date,
        account_new_principal: int) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert len(coordinator_type) <= 30
    assert MIN_INT64 <= other_creditor_id <= MAX_INT64
    assert 0 < transfer_seqnum <= MAX_INT64
    assert committed_amount != 0
    assert -MAX_INT64 <= committed_amount <= MAX_INT64
    assert -MAX_INT64 <= account_new_principal <= MAX_INT64

    db.session.add(CommittedTransfer(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_seqnum=transfer_seqnum,
        coordinator_type=coordinator_type,
        other_creditor_id=other_creditor_id,
        committed_at_ts=committed_at_ts,
        committed_amount=committed_amount,
        transfer_info=transfer_info,
        account_creation_date=account_creation_date,
        account_new_principal=account_new_principal,
    ))
    try:
        db.session.flush()
    except IntegrityError:
        # Normally, this can happen only when the committed transfer
        # message has been re-delivered. Therefore, no action should
        # be taken.
        db.session.rollback()
        return

    ledger = _get_or_create_ledger(creditor_id, debtor_id)
    current_ts = datetime.now(tz=timezone.utc)
    if account_creation_date > ledger.account_creation_date:
        # A new "epoch" has started -- the old ledger must be
        # discarded, and a brand new ledger created.
        ledger.account_creation_date = account_creation_date
        ledger.principal = 0
        ledger.next_transfer_seqnum = (date_to_int24(account_creation_date) << 40) + 1
        ledger.last_update_ts = current_ts

    ledger_has_not_been_updated_soon = current_ts - ledger.last_update_ts > TD_5_SECONDS
    if transfer_seqnum == ledger.next_transfer_seqnum and ledger_has_not_been_updated_soon:
        # If committed transfers come in the right order, it is faster
        # to update the account ledger right away. We must be careful,
        # though, not to update the account ledger too often, because
        # this can cause a row lock contention.
        _update_ledger(ledger, account_new_principal, current_ts)
        _insert_ledger_addition(creditor_id, debtor_id, transfer_seqnum)
    elif transfer_seqnum >= ledger.next_transfer_seqnum:
        # A dedicated asynchronous task will do the addition to the account
        # ledger later. (See `process_pending_committed_transfers()`.)
        db.session.add(PendingCommittedTransfer(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            transfer_seqnum=transfer_seqnum,
            account_new_principal=account_new_principal,
            committed_at_ts=committed_at_ts,
        ))


@atomic
def process_pending_committed_transfers(creditor_id: int, debtor_id: int, max_count: int = None) -> bool:
    """Return `False` if some legible committed transfers remained unprocessed."""

    has_gaps = False
    pks_to_delete = []
    ledger = _get_or_create_ledger(debtor_id, creditor_id, lock=True)
    pending_transfers = _get_ordered_pending_transfers(ledger, max_count)
    for transfer_seqnum, account_new_principal in pending_transfers:
        pk = (creditor_id, debtor_id, transfer_seqnum)
        if transfer_seqnum == ledger.next_transfer_seqnum:
            _update_ledger(ledger, account_new_principal)
            _insert_ledger_addition(*pk)
        elif transfer_seqnum > ledger.next_transfer_seqnum:
            has_gaps = True
            break
        pks_to_delete.append(pk)

    PendingCommittedTransfer.query.\
        filter(PENDING_COMMITTED_TRANSFER_PK.in_(pks_to_delete)).\
        delete(synchronize_session=False)
    return has_gaps or max_count is None or len(pending_transfers) < max_count


def _create_ledger(debtor_id: int, creditor_id: int) -> AccountLedger:
    ledger = AccountLedger(creditor_id=creditor_id, debtor_id=debtor_id)
    with db.retry_on_integrity_error():
        db.session.add(ledger)
    return ledger


def _get_or_create_ledger(creditor_id: int, debtor_id: int, lock: bool = False) -> AccountLedger:
    if lock:
        ledger = AccountLedger.lock_instance((debtor_id, creditor_id))
    else:
        ledger = AccountLedger.get_instance((debtor_id, creditor_id))
    return ledger or _create_ledger(debtor_id, creditor_id)


def _insert_ledger_addition(creditor_id: int, debtor_id: int, transfer_seqnum: int) -> None:
    db.session.add(LedgerAddition(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_seqnum=transfer_seqnum,
    ))


def _update_ledger(ledger: AccountLedger, account_new_principal: int, current_ts: datetime = None) -> None:
    ledger.principal = account_new_principal
    ledger.next_transfer_seqnum += 1
    ledger.last_update_ts = current_ts or datetime.now(tz=timezone.utc)


def _get_ordered_pending_transfers(ledger: AccountLedger, max_count: int = None) -> List[Tuple[int, int]]:
    creditor_id = ledger.creditor_id
    debtor_id = ledger.debtor_id
    query = db.session.\
        query(PendingCommittedTransfer.transfer_seqnum, PendingCommittedTransfer.account_new_principal).\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        order_by(PendingCommittedTransfer.transfer_seqnum)
    if max_count is not None:
        query = query.limit(max_count)
    return query.all()
