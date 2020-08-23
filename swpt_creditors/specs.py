"""JSON snippets to be included in the OpenAPI specification file."""

DID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'string',
        'pattern': '^[0-9A-Za-z_=-]{1,64}$',
    },
}

CID = {
    'in': 'path',
    'name': 'creditorId',
    'required': True,
    'description': "The creditor's ID",
    'schema': {
        'type': 'string',
        'pattern': '^[0-9A-Za-z_=-]{1,64}$',
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

ERROR_CONTENT = {
    'application/json': {
        'schema': {
            'type': 'object',
            'properties': {
                'code': {
                    'type': 'integer',
                    'format': 'int32',
                    'description': 'Error code',
                },
                'errors': {
                    'type': 'object',
                    'description': 'Errors',
                },
                'status': {
                    'type': 'string',
                    'description': 'Error name',
                },
                'message': {
                    'type': 'string',
                    'description': 'Error message',
                }
            }
        }
    }
}

CONFLICTING_CREDITOR = {
    'description': 'A creditor with the same ID already exists.',
    'content': ERROR_CONTENT,
}

TRANSFER_CONFLICT = {
    'description': 'A different transfer entry with the same UUID already exists.',
    'content': ERROR_CONTENT,
}

TRANSFER_CANCELLATION_FAILURE = {
    'description': 'The transfer can not be canceled.',
    'content': ERROR_CONTENT,
}

DENIED_TRANSFER = {
    'description': 'The transfer is forbidden.',
    'content': ERROR_CONTENT,
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

ACCOUNT_DISPLAY_UPDATE_CONFLICT = {
    'description': 'Another account with the same `debtorName` already exists.',
    'content': ERROR_CONTENT,
}

FORBIDDEN_ACCOUNT_OPERATION = {
    'description': 'Forbidden account operation.',
    'content': ERROR_CONTENT,
}

ACCOUNT_EXISTS = {
    'description': 'Account exists.',
    'headers': LOCATION_HEADER,
}

UNSAFE_ACCOUNT_DELETION = {
    'description': 'Forbidden unsafe deletion.',
    'content': ERROR_CONTENT,
}

PEG_ACCOUNT_DELETION = {
    'description': 'The account acts as a currency peg.',
    'content': ERROR_CONTENT,
}

NO_ACCOUNT_WITH_THIS_DEBTOR = {
    "description": "Account does not exist.",
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
