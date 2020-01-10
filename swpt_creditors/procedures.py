from datetime import datetime, date, timedelta, timezone
from uuid import UUID
from typing import TypeVar, Optional, Callable, Tuple, List
from flask import current_app
from sqlalchemy.exc import IntegrityError
from .extensions import db
from .models import CommittedTransfer, PendingCommittedTransfer, InitiatedTransfer, RunningTransfer, \
    MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID

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

    # TODO: Add implementation.
