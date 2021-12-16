from datetime import datetime, timezone
from typing import TypeVar, Callable, List, Optional
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import joinedload
from swpt_lib.utils import increment_seqnum
from swpt_creditors.extensions import db
from swpt_creditors.models import Account, AccountData, ConfigureAccountSignal, \
    AccountDisplay, AccountKnowledge, AccountExchange, LedgerEntry, PendingLogEntry, \
    Creditor, DEFAULT_NEGLIGIBLE_AMOUNT, DEFAULT_CONFIG_FLAGS, uid_seq
from .common import allow_update, get_paths_and_types, LOAD_ONLY_CONFIG_RELATED_COLUMNS, \
    LOAD_ONLY_INFO_RELATED_COLUMNS, LOAD_ONLY_LEDGER_RELATED_COLUMNS
from .creditors import get_active_creditor, _get_creditor, _add_log_entry
from . import errors

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5


@atomic
def get_account_debtor_ids(creditor_id: int, *, count: int = 1, prev: int = None) -> List[int]:
    query = db.session.\
        query(Account.debtor_id).\
        filter(Account.creditor_id == creditor_id).\
        order_by(Account.debtor_id)

    if prev is not None:
        query = query.filter(Account.debtor_id > prev)

    return [t[0] for t in query.limit(count).all()]


@atomic
def has_account(creditor_id: int, debtor_id: int) -> bool:
    account_query = Account.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    return db.session.query(account_query.exists()).scalar()


@atomic
def get_account(creditor_id: int, debtor_id: int) -> Optional[Account]:
    return Account.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(
            joinedload(Account.knowledge, innerjoin=True),
            joinedload(Account.exchange, innerjoin=True),
            joinedload(Account.display, innerjoin=True),
            joinedload(Account.data, innerjoin=True),
        ).\
        one_or_none()


@atomic
def create_new_account(creditor_id: int, debtor_id: int) -> Account:
    current_ts = datetime.now(tz=timezone.utc)

    creditor = get_active_creditor(creditor_id, lock=True)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    if has_account(creditor_id, debtor_id):
        raise errors.AccountExists()

    account = _insert_account(creditor, debtor_id, current_ts)

    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=current_ts,
        seqnum=0,
        negligible_amount=DEFAULT_NEGLIGIBLE_AMOUNT,
        config_flags=DEFAULT_CONFIG_FLAGS,
    ))
    return account


@atomic
def delete_account(creditor_id: int, debtor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    creditor = _get_creditor(creditor_id, lock=True)
    if creditor is None:
        raise errors.CreditorDoesNotExist()

    data = get_account_config(creditor_id, debtor_id)
    if data is None:
        raise errors.AccountDoesNotExist()

    if not (data.is_deletion_safe or data.allow_unsafe_deletion):
        raise errors.UnsafeAccountDeletion()

    pegged_accounts_query = AccountExchange.query.\
        filter_by(creditor_id=creditor_id, peg_debtor_id=debtor_id).\
        filter(AccountExchange.debtor_id != debtor_id)
    if db.session.query(pegged_accounts_query.exists()).scalar():
        raise errors.ForbiddenPegDeletion()

    # NOTE: To guarantee monotonic increase in accounts' ledger entry
    # IDs, even when accounts get deleted and created again, we make
    # sure that `largest_historic_ledger_entry_id` contains the
    # largest ledger entry ID that have been produced for any deleted
    # accounts in the past.
    if data.ledger_last_entry_id > creditor.largest_historic_ledger_entry_id:
        creditor.largest_historic_ledger_entry_id = data.ledger_last_entry_id

    with db.retry_on_integrity_error():
        Account.query.\
            filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
            delete(synchronize_session=False)

    _log_account_deletion(creditor, debtor_id, current_ts)


def get_account_config(creditor_id: int, debtor_id: int, lock=False) -> Optional[AccountData]:
    query = AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(LOAD_ONLY_CONFIG_RELATED_COLUMNS)

    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def update_account_config(
        creditor_id: int,
        debtor_id: int,
        *,
        is_scheduled_for_deletion: bool,
        negligible_amount: float,
        allow_unsafe_deletion: bool,
        latest_update_id: int) -> AccountData:

    current_ts = datetime.now(tz=timezone.utc)

    data = get_account_config(creditor_id, debtor_id, lock=True)
    if data is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(data, 'config_latest_update_id', latest_update_id, {
            'is_scheduled_for_deletion': is_scheduled_for_deletion,
            'negligible_amount': negligible_amount,
            'allow_unsafe_deletion': allow_unsafe_deletion,
        })
    except errors.AlreadyUpToDate:
        return data

    deletion_was_safe_before_the_update = data.is_deletion_safe
    perform_update()
    data.config_latest_update_ts = current_ts
    data.last_config_ts = max(current_ts, data.last_config_ts)
    data.last_config_seqnum = increment_seqnum(data.last_config_seqnum)
    data.is_config_effectual = False

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at=current_ts,
        object_type=types.account_config,
        object_uri=paths.account_config(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))
    db.session.execute(uid_seq)

    assert not data.is_deletion_safe
    if deletion_was_safe_before_the_update:
        _insert_info_update_pending_log_entry(data, current_ts)

    db.session.add(ConfigureAccountSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=data.last_config_ts,
        seqnum=data.last_config_seqnum,
        negligible_amount=data.negligible_amount,
        config_flags=data.config_flags,
    ))

    return data


@atomic
def get_account_display(creditor_id: int, debtor_id: int, lock=False) -> Optional[AccountDisplay]:
    query = AccountDisplay.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def update_account_display(
        creditor_id: int,
        debtor_id: int,
        *,
        debtor_name: Optional[str],
        amount_divisor: float,
        decimal_places: int,
        unit: Optional[str],
        known_debtor: bool,
        latest_update_id: int) -> AccountDisplay:

    current_ts = datetime.now(tz=timezone.utc)

    display = get_account_display(creditor_id, debtor_id, lock=True)
    if display is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(display, 'latest_update_id', latest_update_id, {
            'debtor_name': debtor_name,
            'amount_divisor': amount_divisor,
            'decimal_places': decimal_places,
            'unit': unit,
            'known_debtor': known_debtor,
        })
    except errors.AlreadyUpToDate:
        return display

    if debtor_name not in [display.debtor_name, None]:
        debtor_name_query = AccountDisplay.query.filter_by(creditor_id=creditor_id, debtor_name=debtor_name)
        if db.session.query(debtor_name_query.exists()).scalar():
            raise errors.DebtorNameConflict()

    with db.retry_on_integrity_error():
        perform_update()
        display.latest_update_ts = current_ts

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at=current_ts,
        object_type=types.account_display,
        object_uri=paths.account_display(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))
    db.session.execute(uid_seq)

    return display


@atomic
def get_account_knowledge(creditor_id: int, debtor_id: int, lock=False) -> Optional[AccountKnowledge]:
    query = AccountKnowledge.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def update_account_knowledge(
        creditor_id: int,
        debtor_id: int,
        *,
        latest_update_id: int,
        data: dict) -> AccountKnowledge:

    current_ts = datetime.now(tz=timezone.utc)

    knowledge = get_account_knowledge(creditor_id, debtor_id, lock=True)
    if knowledge is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(knowledge, 'latest_update_id', latest_update_id, {'data': data})
    except errors.AlreadyUpToDate:
        return knowledge

    perform_update()
    knowledge.latest_update_ts = current_ts

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at=current_ts,
        object_type=types.account_knowledge,
        object_uri=paths.account_knowledge(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))
    db.session.execute(uid_seq)

    return knowledge


@atomic
def get_account_exchange(creditor_id: int, debtor_id: int, lock=False) -> Optional[AccountExchange]:
    query = AccountExchange.query.filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


@atomic
def update_account_exchange(
        creditor_id: int,
        debtor_id: int,
        *,
        policy: Optional[str],
        min_principal: int,
        max_principal: int,
        peg_exchange_rate: Optional[float],
        peg_debtor_id: Optional[int],
        latest_update_id: int) -> AccountKnowledge:

    current_ts = datetime.now(tz=timezone.utc)

    exchange = get_account_exchange(creditor_id, debtor_id, lock=True)
    if exchange is None:
        raise errors.AccountDoesNotExist()

    try:
        perform_update = allow_update(exchange, 'latest_update_id', latest_update_id, {
            'policy': policy,
            'min_principal': min_principal,
            'max_principal': max_principal,
            'peg_exchange_rate': peg_exchange_rate,
            'peg_debtor_id': peg_debtor_id,
        })
    except errors.AlreadyUpToDate:
        return exchange

    if policy not in [None, 'conservative']:
        raise errors.InvalidPolicyName()

    if peg_debtor_id is not None and not has_account(creditor_id, peg_debtor_id):
        raise errors.PegDoesNotExist()

    perform_update()
    exchange.latest_update_ts = current_ts

    paths, types = get_paths_and_types()
    db.session.add(PendingLogEntry(
        creditor_id=creditor_id,
        added_at=current_ts,
        object_type=types.account_exchange,
        object_uri=paths.account_exchange(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=latest_update_id,
    ))
    db.session.execute(uid_seq)

    return exchange


@atomic
def get_account_info(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(LOAD_ONLY_INFO_RELATED_COLUMNS).\
        one_or_none()


@atomic
def get_account_ledger(creditor_id: int, debtor_id: int) -> Optional[AccountData]:
    return AccountData.query.\
        filter_by(creditor_id=creditor_id, debtor_id=debtor_id).\
        options(LOAD_ONLY_LEDGER_RELATED_COLUMNS).\
        one_or_none()


@atomic
def get_account_ledger_entries(
        creditor_id: int,
        debtor_id: int,
        *,
        prev: int,
        stop: int = 0,
        count: int = 1) -> List[LedgerEntry]:

    return LedgerEntry.query.\
        filter(
            LedgerEntry.creditor_id == creditor_id,
            LedgerEntry.debtor_id == debtor_id,
            LedgerEntry.entry_id < prev,
            LedgerEntry.entry_id > stop,
        ).\
        order_by(LedgerEntry.entry_id.desc()).\
        limit(count).\
        all()


def _insert_account(creditor: Creditor, debtor_id: int, current_ts: datetime) -> Account:
    creditor_id = creditor.creditor_id

    data = AccountData(
        last_config_ts=current_ts,
        last_config_seqnum=0,
        config_latest_update_ts=current_ts,
        info_latest_update_ts=current_ts,
        ledger_latest_update_ts=current_ts,
        ledger_last_entry_id=creditor.largest_historic_ledger_entry_id + 1  # a gap
    )
    account = Account(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        created_at=current_ts,
        knowledge=AccountKnowledge(latest_update_ts=current_ts),
        exchange=AccountExchange(latest_update_ts=current_ts),
        display=AccountDisplay(latest_update_ts=current_ts),
        data=data,
        latest_update_ts=current_ts,
    )
    db.session.add(account)
    db.session.flush()

    paths, types = get_paths_and_types()
    _add_log_entry(
        creditor,
        object_type=types.account,
        object_uri=paths.account(creditorId=creditor_id, debtorId=debtor_id),
        object_update_id=account.latest_update_id,
        added_at=current_ts,
    )

    creditor.accounts_list_latest_update_id += 1
    creditor.accounts_list_latest_update_ts = current_ts
    _add_log_entry(
        creditor,
        object_type=types.accounts_list,
        object_uri=paths.accounts_list(creditorId=creditor_id),
        object_update_id=creditor.accounts_list_latest_update_id,
        added_at=current_ts,
    )

    return account


def _log_account_deletion(creditor: Creditor, debtor_id: int, current_ts: datetime) -> None:
    creditor_id = creditor.creditor_id
    paths, types = get_paths_and_types()
    object_update_id = db.session.execute(uid_seq)

    creditor.accounts_list_latest_update_id += 1
    creditor.accounts_list_latest_update_ts = current_ts
    _add_log_entry(
        creditor,
        object_type=types.accounts_list,
        object_uri=paths.accounts_list(creditorId=creditor_id),
        object_update_id=creditor.accounts_list_latest_update_id,
        added_at=current_ts,
    )

    for object_type, object_uri in [
        (types.account, paths.account(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_config, paths.account_config(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_info, paths.account_info(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_ledger, paths.account_ledger(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_display, paths.account_display(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_exchange, paths.account_exchange(creditorId=creditor_id, debtorId=debtor_id)),
        (types.account_knowledge, paths.account_knowledge(creditorId=creditor_id, debtorId=debtor_id)),
    ]:
        _add_log_entry(
            creditor,
            object_type=object_type,
            object_uri=object_uri,
            object_update_id=object_update_id,
            added_at=current_ts,
            is_deleted=True,
        )


def _insert_info_update_pending_log_entry(data: AccountData, current_ts: datetime) -> None:
    paths, types = get_paths_and_types()
    data.info_latest_update_id += 1
    data.info_latest_update_ts = current_ts

    db.session.add(PendingLogEntry(
        creditor_id=data.creditor_id,
        added_at=current_ts,
        object_type=types.account_info,
        object_uri=paths.account_info(creditorId=data.creditor_id, debtorId=data.debtor_id),
        object_update_id=data.info_latest_update_id,
    ))
    db.session.execute(uid_seq)
