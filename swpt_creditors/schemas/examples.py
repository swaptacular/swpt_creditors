ACCOUNT_LEDGER_ENTRIES_EXAMPLE = {
    'uri': '/creditors/2/accounts/1/entries?prev=124',
    'type': 'LedgerEntriesPage',
    'items': [
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/2/accounts/1/ledger'},
            'transfer': {'uri': '/creditors/2/accounts/1/transfers/18444-999'},
            'entryId': 123,
            'addedAt': '2020-04-03T18:42:44Z',
            'principal': 1500,
            'aquiredAmount': 1000
        },
    ],
    'next': '?prev=123',
}

LOG_ENTRIES_EXAMPLE = {
    'uri': '/creditors/2/log',
    'type': 'LogEntriesPage',
    'items': [
        {
            'type': 'LogEntry',
            'entryId': 12345,
            'addedAt': '2020-04-06T14:22:11Z',
            'objectType': 'Account',
            'object': {'uri': '/creditors/2/accounts/1/'},
            'objectUpdateId': 10,
            'deleted': False,
        },
    ],
    'forthcoming': '?prev=12345',
}

TRANSFER_LINKS_EXAMPLE = {
    'next': '?prev=00112233-4455-6677-8899-aabbccddeeff',
    'items': [
        {'uri': '123e4567-e89b-12d3-a456-426655440000'},
        {'uri': '00112233-4455-6677-8899-aabbccddeeff'},
    ],
    'uri': '/creditors/2/transfers/',
    'type': 'ObjectReferencesPage',
}

ACCOUNT_IDENTITY_EXAMPLE = {
    'type': 'AccountIdentity',
    'uri': 'swpt:1/2222',
}

DEBTOR_IDENTITY_EXAMPLE = {
    'type': 'DebtorIdentity',
    'uri': 'swpt:1',
}

ACCOUNTS_LIST_EXAMPLE = {
    'wallet': {'uri': '/creditors/2/wallet'},
    'type': 'AccountsList',
    'uri': '/creditors/2/accounts-list',
    'first': '/creditors/2/accounts/',
    'itemsType': 'ObjectReference',
    'latestUpdateId': 777,
    'latestUpdateAt': '2020-06-20T18:53:43Z',
}

TRANSFERS_LIST_EXAMPLE = {
    'wallet': {'uri': '/creditors/2/wallet'},
    'type': 'TransfersList',
    'uri': '/creditors/2/transfers-list',
    'first': '/creditors/2/transfers/',
    'itemsType': 'ObjectReference',
    'latestUpdateId': 778,
    'latestUpdateAt': '2020-06-20T18:53:43Z',
}

CREDITORS_LIST_EXAMPLE = {
    'type': 'CreditorsList',
    'uri': '/creditors-list',
    'itemsType': 'ObjectReference',
    'first': '/creditors/9223372036854775808/enumerate/',
}
