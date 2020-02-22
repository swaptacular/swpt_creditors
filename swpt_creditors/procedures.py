from datetime import datetime, date, timedelta, timezone
from typing import TypeVar, Callable, Tuple, List
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import tuple_
from sqlalchemy.orm import joinedload
from swpt_lib.utils import date_to_int24, is_later_event, increment_seqnum
from .extensions import db
from .models import AccountLedger, LedgerEntry, AccountCommit,  \
    Account, AccountConfig, ConfigureAccountSignal, PendingAccountCommit, \
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, BEGINNING_OF_TIME

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_5_SECONDS = timedelta(seconds=10)
PENDING_ACCOUNT_COMMIT_PK = tuple_(
    PendingAccountCommit.debtor_id,
    PendingAccountCommit.creditor_id,
    PendingAccountCommit.transfer_seqnum,
)


@atomic
def process_account_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_ts: datetime,
        change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        last_transfer_seqnum: int,
        last_config_change_ts: datetime,
        last_config_change_seqnum: int,
        creation_date: date,
        negligible_amount: float,
        status: int) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert change_ts is not None
    assert MIN_INT32 <= change_seqnum <= MAX_INT32
    assert -MAX_INT64 <= principal <= MAX_INT64
    assert INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL
    assert 0 <= last_transfer_seqnum <= MAX_INT64
    assert last_config_change_ts is not None
    assert MIN_INT32 <= last_config_change_seqnum <= MAX_INT32
    assert creation_date is not None
    assert negligible_amount >= 2.0
    assert MIN_INT16 <= status <= MAX_INT16

    account = Account.lock_instance((debtor_id, creditor_id), joinedload('account_config', innerjoin=True))
    if account:
        this_event = (change_ts, change_seqnum)
        prev_event = (account.change_ts, account.change_seqnum)
        if this_event == prev_event:
            account.last_heartbeat_ts = datetime.now(tz=timezone.utc)
        if not is_later_event(this_event, prev_event):
            return
        account.change_seqnum = change_seqnum
        account.change_ts = change_ts
        account.principal = principal
        account.interest = interest
        account.interest_rate = interest_rate
        account.last_transfer_seqnum = last_transfer_seqnum
        account.last_config_change_ts = last_config_change_ts
        account.last_config_change_seqnum = last_config_change_seqnum
        account.creation_date = creation_date
        account.negligible_amount = negligible_amount
        account.status = status
        account.last_heartbeat_ts = datetime.now(tz=timezone.utc)
        config = account.account_config
    else:
        account = Account(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            change_seqnum=change_seqnum,
            change_ts=change_ts,
            principal=principal,
            interest=interest,
            interest_rate=interest_rate,
            last_transfer_seqnum=last_transfer_seqnum,
            last_config_change_ts=last_config_change_ts,
            last_config_change_seqnum=last_config_change_seqnum,
            creation_date=creation_date,
            negligible_amount=negligible_amount,
            status=status,
        )
        config = _touch_account_config(creditor_id, debtor_id, account=account)
        with db.retry_on_integrity_error():
            db.session.add(account)

    _revise_account_config(account, config)


@atomic
def process_account_commit_signal(
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

    db.session.add(AccountCommit(
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
        # Normally, this can happen only when the account commit
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
        # If account commits come in the right order, it is faster to
        # update the account ledger right away. We must be careful,
        # though, not to update the account ledger too often, because
        # this can cause a row lock contention.
        _update_ledger(ledger, account_new_principal, current_ts)
        _insert_ledger_entry(creditor_id, debtor_id, transfer_seqnum, committed_amount, account_new_principal)
    elif transfer_seqnum >= ledger.next_transfer_seqnum:
        # A dedicated asynchronous task will do the addition to the account
        # ledger later. (See `process_pending_account_commits()`.)
        db.session.add(PendingAccountCommit(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            transfer_seqnum=transfer_seqnum,
            account_new_principal=account_new_principal,
            committed_at_ts=committed_at_ts,
            committed_amount=committed_amount,
        ))


@atomic
def process_pending_account_commits(creditor_id: int, debtor_id: int, max_count: int = None) -> bool:
    """Return `False` if some legible account commits remained unprocessed."""

    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    has_gaps = False
    pks_to_delete = []
    ledger = _get_or_create_ledger(debtor_id, creditor_id, lock=True)
    pending_transfers = _get_ordered_pending_transfers(ledger, max_count)
    for transfer_seqnum, committed_amount, account_new_principal in pending_transfers:
        pk = (creditor_id, debtor_id, transfer_seqnum)
        if transfer_seqnum == ledger.next_transfer_seqnum:
            _update_ledger(ledger, account_new_principal)
            _insert_ledger_entry(*pk, committed_amount, account_new_principal)
        elif transfer_seqnum > ledger.next_transfer_seqnum:
            has_gaps = True
            break
        pks_to_delete.append(pk)

    PendingAccountCommit.query.\
        filter(PENDING_ACCOUNT_COMMIT_PK.in_(pks_to_delete)).\
        delete(synchronize_session=False)
    return has_gaps or max_count is None or len(pending_transfers) < max_count


@atomic
def find_legible_pending_account_commits(max_count: int = None):
    pac = PendingAccountCommit
    al = AccountLedger
    query = db.session.query(pac.creditor_id, pac.debtor_id).filter(
        al.creditor_id == pac.creditor_id,
        al.debtor_id == pac.debtor_id,
        al.next_transfer_seqnum == pac.transfer_seqnum
    )
    if max_count is not None:
        query = query.limit(max_count)
    return query.all()


@atomic
def create_or_reset_account_config(creditor_id: int, debtor_id: int) -> Tuple[AccountConfig, bool]:
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64

    config = AccountConfig.lock_instance((creditor_id, debtor_id))
    config_should_be_created = config is None
    if config_should_be_created:
        config = _create_account_config_instance(creditor_id, debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(config)
        _insert_configure_account_signal(config)
    else:
        config.reset()
    return config, config_should_be_created


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


def _insert_ledger_entry(
        creditor_id: int,
        debtor_id: int,
        transfer_seqnum: int,
        committed_amount: int,
        account_new_principal: int) -> None:

    db.session.add(LedgerEntry(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        transfer_seqnum=transfer_seqnum,
        committed_amount=committed_amount,
        account_new_principal=account_new_principal,
    ))


def _update_ledger(ledger: AccountLedger, account_new_principal: int, current_ts: datetime = None) -> None:
    ledger.principal = account_new_principal
    ledger.next_transfer_seqnum += 1
    ledger.last_update_ts = current_ts or datetime.now(tz=timezone.utc)


def _get_ordered_pending_transfers(ledger: AccountLedger, max_count: int = None) -> List[Tuple[int, int]]:
    creditor_id = ledger.creditor_id
    debtor_id = ledger.debtor_id
    query = db.session.\
        query(
            PendingAccountCommit.transfer_seqnum,
            PendingAccountCommit.committed_amount,
            PendingAccountCommit.account_new_principal,
        ).\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        order_by(PendingAccountCommit.transfer_seqnum)
    if max_count is not None:
        query = query.limit(max_count)
    return query.all()


def _touch_account_config(
        creditor_id: int,
        debtor_id: int,
        account: Account,
        reset_ledger: bool = False) -> AccountConfig:

    # TODO: Can this be simplified? Perhaps some of the logic can be
    #       moved in process_account_change_signal().

    config = AccountConfig.lock_instance((creditor_id, debtor_id))
    if config is None:
        config = _create_account_config_instance(creditor_id, debtor_id)
        _revise_account_config(account, config)
        with db.retry_on_integrity_error():
            db.session.add(config)
        reset_ledger = True

    if reset_ledger:
        assert account.creditor_id == creditor_id and account.debtor_id == debtor_id
        ledger = config.account_ledger
        ledger.account_creation_date = account.creation_date
        ledger.principal = account.principal
        ledger.next_transfer_seqnum = account.last_transfer_seqnum + 1
        ledger.last_update_ts = datetime.now(tz=timezone.utc)

    return config


def _create_account_config_instance(creditor_id: int, debtor_id: int) -> AccountConfig:
    return AccountConfig(account_ledger=_get_or_create_ledger(creditor_id, debtor_id, lock=True))


def _insert_configure_account_signal(config: AccountConfig, current_ts: datetime = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    config.last_change_ts = max(config.last_change_ts, current_ts)
    config.last_change_seqnum = increment_seqnum(config.last_change_seqnum)
    db.session.add(ConfigureAccountSignal(
        creditor_id=config.creditor_id,
        debtor_id=config.debtor_id,
        change_ts=config.last_change_ts,
        change_seqnum=config.last_change_seqnum,
        negligible_amount=config.negligible_amount,
        is_scheduled_for_deletion=config.is_scheduled_for_deletion,
    ))


def _revise_account_config(account: Account, config: AccountConfig) -> None:
    # We should be careful here, because `config` could be a transient
    # instance, in which case most of its attributes will be `None`.

    account_event = (account.last_config_change_ts, account.last_config_change_seqnum)
    config_event = (config.last_change_ts, config.last_change_seqnum)
    config_is_inadequate = is_later_event(account_event, config_event)
    if config_is_inadequate or not config.is_effectual and not is_later_event(config_event, account_event):
        last_change_ts = config.last_change_ts or BEGINNING_OF_TIME
        config.is_effectual = True
        config.last_change_ts = max(last_change_ts, account.last_config_change_ts)
        config.last_change_seqnum = account.last_config_change_seqnum
        config.is_scheduled_for_deletion = account.is_scheduled_for_deletion
        config.negligible_amount = account.negligible_amount
