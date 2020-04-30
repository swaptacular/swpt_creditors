from marshmallow import Schema, fields
from .common import ObjectReferenceSchema, LedgerEntrySchema, MessageSchema, URI_DESCRIPTION

_PAGE_NEXT_DESCRIPTION = '\
An URI of another `{type}` object which contains more items. When \
there are no remaining items, this field will not be present. If this field \
is present, there might be remaining items, even when the `items` array is \
empty. This can be a relative URI.'

_PAGE_FORTHCOMING_DESCRIPTION = '\
An URI of another `{type}` object which would contain items that \
might be added in the future. That is: items that are not currently available, \
but may become available in the future. This is useful when we want to follow \
a continuous stream of new items. This field will not be present when the \
`next` field is present. This can be a relative URI.'


class PaginationParametersSchema(Schema):
    prev = fields.String(
        description='Return items which follow the item with this index.',
        example='0',
    )


class MessagesPageSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/log',
    )
    type = fields.Function(
        lambda obj: 'MessagesPage',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='MessagesPage',
    )
    items = fields.Nested(
        MessageSchema(many=True),
        required=True,
        dump_only=True,
        description='An array of messages. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=_PAGE_NEXT_DESCRIPTION.format(type='MessagesPage'),
        example='?prev=12345',
    )
    forthcoming = fields.Method(
        'get_forthcoming_uri',
        type='string',
        format='uri-reference',
        description=_PAGE_FORTHCOMING_DESCRIPTION.format(type='MessagesPage'),
        example='?prev=1234567890',
    )


class ObjectReferencesPage(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/',
    )
    type = fields.Function(
        lambda obj: 'ObjectReferencesPage',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='ObjectReferencesPage',
    )
    items = fields.Nested(
        ObjectReferenceSchema(many=True),
        required=True,
        dump_only=True,
        description='An array of object references. Can be empty.',
        example=[{'uri': f'{i}/'} for i in [1, 11, 111]],
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=_PAGE_NEXT_DESCRIPTION.format(type='ObjectReferencesPage'),
        example='?prev=111',
    )


class LedgerEntriesPage(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/entries?prev=124',
    )
    type = fields.Function(
        lambda obj: 'LedgerEntriesPage',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntriesPage',
    )
    items = fields.Nested(
        LedgerEntrySchema(many=True),
        required=True,
        dump_only=True,
        description='An array of ledger entries. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=_PAGE_NEXT_DESCRIPTION.format(type='LedgerEntriesPage'),
    )
    forthcoming = fields.Method(
        'get_forthcoming_uri',
        type='string',
        format='uri-reference',
        description=_PAGE_FORTHCOMING_DESCRIPTION.format(type='LedgerEntriesPage'),
    )
