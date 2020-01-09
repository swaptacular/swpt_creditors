"""JSON snippets to be included in the OpenAPI specification file."""

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

TOO_MANY_TRANSFERS = {
    'description': 'Too many issuing transfers.',
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}
