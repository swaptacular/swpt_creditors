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

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

FORBIDDEN_OPERATION = {
    'description': 'Forbidden operation.',
    'content': ERROR_CONTENT,
}

UPDATE_CONFLICT = {
    'description': 'Conflicting update attempts.',
    'content': ERROR_CONTENT,
}

ACCOUNT_EXISTS = {
    'description': 'Account exists.',
    'headers': LOCATION_HEADER,
}

FORBIDDEN_ACCOUNT_DELETION = {
    'description': 'Forbidden account deletion.',
    'content': ERROR_CONTENT,
}

PEG_ACCOUNT_DELETION = {
    'description': 'The account acts as a currency peg.',
    'content': ERROR_CONTENT,
}

NO_ACCOUNT_WITH_THIS_DEBTOR = {
    "description": "Account does not exist.",
}
