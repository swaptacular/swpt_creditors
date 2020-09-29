from functools import partial
from typing import Tuple
from datetime import date, timedelta, datetime, timezone
from flask import url_for, current_app, request
from flask_smorest import abort
from swpt_lib.utils import u64_to_i64
from swpt_creditors.models import MAX_INT64, DATE0, Pin
from swpt_creditors.schemas import type_registry


def make_transfer_slug(creation_date: date, transfer_number: int) -> str:
    epoch = (creation_date - DATE0).days
    return f'{epoch}-{transfer_number}'


def parse_transfer_slug(slug) -> Tuple[date, int]:
    epoch, transfer_number = slug.split('-', maxsplit=1)
    epoch = int(epoch)
    transfer_number = int(transfer_number)

    try:
        creation_date = DATE0 + timedelta(days=epoch)
    except OverflowError:
        raise ValueError from None

    if not 1 <= transfer_number <= MAX_INT64:
        raise ValueError

    return creation_date, transfer_number


def calc_checkup_datetime(debtor_id: int, initiated_at: datetime) -> datetime:
    current_ts = datetime.now(tz=timezone.utc)
    current_delay = current_ts - initiated_at
    average_delay = timedelta(seconds=float(current_app.config['APP_TRANSFERS_FINALIZATION_AVG_SECONDS']))
    return current_ts + max(current_delay, average_delay)


def calc_log_retention_days(creditor_id: int) -> int:
    return int(current_app.config['APP_LOG_RETENTION_DAYS'])


def calc_reservation_deadline(created_at: datetime) -> datetime:
    return created_at + timedelta(days=current_app.config['APP_INACTIVE_CREDITOR_RETENTION_DAYS'])


def calc_require_pin(pin: Pin) -> bool:
    NOT_REQUIED = 'false'

    x_swpt_require_pin = request.headers.get('X-Swpt-Require-Pin', NOT_REQUIED)
    is_in_pin_reset_mode = x_swpt_require_pin == NOT_REQUIED
    return not is_in_pin_reset_mode and pin.is_required


def verify_creditor_id():
    ADMIN = '*'

    creditor_id = request.headers.get('X-Swpt-Creditor-Id', ADMIN)
    if creditor_id != ADMIN and u64_to_i64(int(creditor_id)) != request.view_args['creditorId']:
        abort(401)


class path_builder:
    def _build_committed_transfer_path(creditorId, debtorId, creationDate, transferNumber):
        return url_for(
            'transfers.CommittedTransferEndpoint',
            creditorId=creditorId,
            debtorId=debtorId,
            transferId=make_transfer_slug(creationDate, transferNumber),
            _external=False,
        )

    def _url_for(name):
        return staticmethod(partial(url_for, name, _external=False))

    creditors_list = _url_for('admin.CreditorsListEndpoint')
    creditor_enumerate = _url_for('admin.CreditorEnumerateEndpoint')
    creditor = _url_for('creditors.CreditorEndpoint')
    pin_state = _url_for('creditors.PinStateEndpoint')
    wallet = _url_for('creditors.WalletEndpoint')
    log_entries = _url_for('creditors.LogEntriesEndpoint')
    debtor_lookup = _url_for('accounts.DebtorLookupEndpoint')
    account_lookup = _url_for('accounts.AccountLookupEndpoint')
    account = _url_for('accounts.AccountEndpoint')
    account_info = _url_for('accounts.AccountInfoEndpoint')
    account_config = _url_for('accounts.AccountConfigEndpoint')
    account_display = _url_for('accounts.AccountDisplayEndpoint')
    account_exchange = _url_for('accounts.AccountExchangeEndpoint')
    account_knowledge = _url_for('accounts.AccountKnowledgeEndpoint')
    account_ledger = _url_for('accounts.AccountLedgerEndpoint')
    account_ledger_entries = _url_for('accounts.AccountLedgerEntriesEndpoint')
    accounts_list = _url_for('creditors.AccountsListEndpoint')
    accounts = _url_for('accounts.AccountsEndpoint')
    transfer = _url_for('transfers.TransferEndpoint')
    transfers_list = _url_for('creditors.TransfersListEndpoint')
    transfers = _url_for('transfers.TransfersEndpoint')
    committed_transfer = _build_committed_transfer_path


context = {
    'paths': path_builder,
    'types': type_registry,
    'calc_checkup_datetime': calc_checkup_datetime,
    'calc_log_retention_days': calc_log_retention_days,
    'calc_reservation_deadline': calc_reservation_deadline,
    'calc_require_pin': calc_require_pin,
}
