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

SEQNUM = {
    'in': 'path',
    'name': 'seqnum',
    'required': True,
    'description': "The sequential number of the transfer",
    'schema': {
        'type': 'integer',
        'format': 'uint64',
        'minimum': 0,
        'maximum': (1 << 64) - 1,
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

CREDITOR_DOES_NOT_EXIST = {
    'description': 'The creditor does not exist.',
}

CONFLICTING_CREDITOR = {
    'description': 'A creditor with the same ID already exists.',
}

TRANSFER_DOES_NOT_EXIST = {
    'description': 'The transfer entry does not exist.',
}

TRANSFER_CONFLICT = {
    'description': 'A different transfer entry with the same UUID already exists.',
}

TRANSFER_UPDATE_CONFLICT = {
    'description': 'The requested transfer update is not possible.',
}

TOO_MANY_TRANSFERS = {
    'description': 'Too many issuing transfers.',
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

INVALID_TRANSFER_CREATION_REQUEST = {
    'description': "The transfer can not be created. Verify recipient's and sender's accounts.",
}

ACCOUNT_DOES_NOT_EXIST = {
    'description': 'The account does not exist.',
}

ACCOUNT_CONFLICT = {
    'description': 'A different account entry with the same debtor ID already exists.',
}

ACCOUNT_UPDATE_CONFLICT = {
    'description': 'The requested account update is not possible.',
}

TOO_MANY_ACCOUNTS = {
    'description': 'Too many existing accounts.',
}

ACCOUNT_EXISTS = {
    'description': 'The same account entry already exists.',
    'headers': LOCATION_HEADER,
}

ACCOUNT_CAN_NOT_BE_CREATED = {
    'description': "The account can not be created. The debtor's info might be wrong.",
}

ACCOUNT_DELETION_NOT_ALLOWED = {
    'description': 'Unsafe deletion of this account is not allowed.',
}

ACCOUNT_DOES_NOT_EXIST = {
    'description': 'The account does not exist.',
}

NO_MATCHING_ACCOUNT = {
    'description': 'No matching account.',
}

ACCOUNT_LEDGER_ENTRIES_EXAMPLE = {
    'uri': '/creditors/2/accounts/1/entries?prev=124',
    'type': 'LedgerEntriesPage',
    'items': [
        {
            'type': 'LedgerEntry',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'transfer': {'uri': '/creditors/2/accounts/1/transfers/999'},
            'entryId': 123,
            'postedAt': '2020-04-03T18:42:44Z',
            'principal': 1500,
            'previousEntryId': 122,
            'amount': 1000
        },
    ],
    'next': '?prev=123',
}

LOG_ENTRIES_EXAMPLE = {
    'uri': '/creditors/2/log',
    'type': 'LogEntriesPage',
    'items': [
        {
            'entryId': 12345,
            'postedAt': '2020-04-06T14:22:11Z',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'type': 'AccountUpdate',
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

FIND_ACCOUNT_REQUEST_EXAMPLE = {
    'type': 'SwptAccountInfo',
    'debtorId': 1,
    'creditorId': 2222
}
