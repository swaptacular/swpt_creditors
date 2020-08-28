from functools import partial
from typing import Tuple
from datetime import date, timedelta
from flask import url_for
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
    committed_transfer = 'CommittedTransfer'


context = {'paths': path_builder}
