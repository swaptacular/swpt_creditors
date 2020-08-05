"""JSON snippets to be included in the OpenAPI specification file."""

DID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'integer',
        'format': 'uint64',
        'minimum': 0,
        'maximum': (1 << 64) - 1,
    },
}

CID = {
    'in': 'path',
    'name': 'creditorId',
    'required': True,
    'description': "The creditor's ID",
    'schema': {
        'type': 'integer',
        'format': 'uint64',
        'minimum': 0,
        'maximum': (1 << 64) - 1,
    },
}

TID = {
    'in': 'path',
    'name': 'transferId',
    'required': True,
    'description': "The transfer's ID",
    'schema': {
        'type': 'string',
        'pattern': '^[0-9A-Za-z_=-]{1,64}$',
    },
}

TRANSFER_UUID = {
    'in': 'path',
    'name': 'transferUuid',
    'required': True,
    'description': "The transfer's UUID",
    'schema': {
        'type': 'string',
        'format': 'uuid',
    },
}

LOCATION_HEADER = {
    'Location': {
        'description': 'The URI of the entry.',
        'schema': {
            'type': 'string',
            'format': 'uri',
        },
    },
}

CONFLICTING_CREDITOR = {
    'description': 'A creditor with the same ID already exists.',
}

TRANSFER_CONFLICT = {
    'description': 'A different transfer entry with the same UUID already exists.',
}

TRANSFER_CANCELLATION_FAILURE = {
    'description': 'The transfer can not be canceled.',
}

DENIED_TRANSFER = {
    'description': 'The transfer is forbidden.',
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

ACCOUNT_DISPLAY_UPDATE_CONFLICT = {
    'description': 'Another account with the same `debtorName` or `ownUnit` already exists.',
}

DENIED_ACCOUNT_CREATION = {
    'description': 'The account creation is forbidden.',
}

ACCOUNT_EXISTS = {
    'description': 'Account does exist.',
    'headers': LOCATION_HEADER,
}

UNSAFE_ACCOUNT_DELETION = {
    'description': 'Unsafe deletion of this account is forbidden.',
}

PEG_ACCOUNT_DELETION = {
    'description': 'This account acts as a currency peg, unpeg the pegged accounts first.',
}

NO_ACCOUNT_WITH_THIS_DEBTOR = {
    "description": "Account does not exist, but the debtor's identity has been recognized.",
}

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
            'previousEntryId': 122,
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
            'previousEntryId': 12344,
            'addedAt': '2020-04-06T14:22:11Z',
            'objectType': 'Account',
            'object': {'uri': '/creditors/2/accounts/1/'},
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

ACCOUNT_LIST_EXAMPLE = {
    'wallet': {'uri': '/creditors/2/wallet'},
    'type': 'AccountList',
    'uri': '/creditors/2/account-list',
    'first': '/creditors/2/accounts/',
    'itemsType': 'ObjectReference',
    'latestUpdateId': 777,
    'latestUpdateAt': '2020-06-20T18:53:43Z',
}

TRANSFER_LIST_EXAMPLE = {
    'wallet': {'uri': '/creditors/2/wallet'},
    'type': 'TransferList',
    'uri': '/creditors/2/transfer-list',
    'first': '/creditors/2/transfers/',
    'itemsType': 'ObjectReference',
    'latestUpdateId': 778,
    'latestUpdateAt': '2020-06-20T18:53:43Z',
}
