from functools import partial
from typing import Tuple
from datetime import date, timedelta, datetime, timezone
from flask import url_for, current_app
from swpt_creditors.models import MAX_INT64, DATE0


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


def calc_checkup_datetime(debtor_id: int, initiated_at_ts: datetime) -> datetime:
    current_ts = datetime.now(tz=timezone.utc)
    current_delay = current_ts - initiated_at_ts
    average_delay = timedelta(seconds=current_app.config['APP_TRANSFERS_FINALIZATION_AVG_SECONDS'])
    return current_ts + max(current_delay, average_delay)


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

    creditor = _url_for('creditors.CreditorEndpoint')
    wallet = _url_for('creditors.WalletEndpoint')
    log_entries = _url_for('creditors.LogEntriesEndpoint')
    account_list = _url_for('creditors.AccountListEndpoint')
    transfer_list = _url_for('creditors.TransferListEndpoint')
    account = _url_for('accounts.AccountEndpoint')
    account_info = _url_for('accounts.AccountInfoEndpoint')
    account_ledger = _url_for('accounts.AccountLedgerEndpoint')
    account_display = _url_for('accounts.AccountDisplayEndpoint')
    account_exchange = _url_for('accounts.AccountExchangeEndpoint')
    account_knowledge = _url_for('accounts.AccountKnowledgeEndpoint')
    account_config = _url_for('accounts.AccountConfigEndpoint')
    account_ledger_entries = _url_for('accounts.AccountLedgerEntriesEndpoint')
    accounts = _url_for('accounts.AccountsEndpoint')
    account_lookup = _url_for('accounts.AccountLookupEndpoint')
    debtor_lookup = _url_for('accounts.DebtorLookupEndpoint')
    transfer = _url_for('transfers.TransferEndpoint')
    transfers = _url_for('transfers.TransfersEndpoint')
    committed_transfer = _build_committed_transfer_path


class schema_types:
    creditor = 'Creditor'
    account = 'Account'
    account_knowledge = 'AccountKnowledge'
    account_exchange = 'AccountExchange'
    account_display = 'AccountDisplay'
    account_config = 'AccountConfig'
    account_info = 'AccountInfo'
    account_ledger = 'AccountLedger'
    account_list = 'AccountList'
    transfer = 'Transfer'
    transfer_list = 'TransferList'
    committed_transfer = 'CommittedTransfer'


context = {
    'paths': path_builder,
    'calc_checkup_datetime': calc_checkup_datetime,
}
