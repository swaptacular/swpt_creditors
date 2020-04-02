"""JSON snippets to be included in the OpenAPI specification file."""

DEBTOR_ID = {
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

CREDITOR_ID = {
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

FIRST = {
    'in': 'query',
    'name': 'first',
    'required': True,
    'description': "Will return only ledger entries with IDs smaller or equal to this value.",
    'schema': {
        'type': 'integer',
        'format': 'uint64',
        'minimum': 0,
        'maximum': (1 << 64) - 1,
    },
}

FIRST_DEBTOR_ID = {
    'in': 'query',
    'name': 'first',
    'required': True,
    'description': "Will return only URIs of account records with debtors IDs bigger or equal to this value.",
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

PAGE_DOES_NOT_EXIST = {
    'description': 'The page does not exist.',
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

ACCOUNT_DOES_NOT_EXIST = {
    'description': 'The account entry does not exist.',
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
    'description': "The account can not be created. The debtor's URI might be wrong.",
}
