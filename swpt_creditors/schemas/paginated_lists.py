from marshmallow import Schema, fields, validate
from swpt_lib import endpoints
from .common import MAX_INT64, MAX_UINT64


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


class PaginatedListSchema(Schema):
    type = fields.Function(
        lambda obj: 'PaginatedList',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='PaginatedList',
    )
    itemsType = fields.String(
        required=True,
        dump_only=True,
        description='The type of the items in the paginated list.',
        example='string',
    )
    first = fields.String(
        required=True,
        dump_only=True,
        format="uri-reference",
        description='The URI of the first page in the paginated list. The object retrieved from '
                    'this URI will have: 1) An `items` property (an array), which will contain the '
                    'first items of the paginated list; 2) May have a `next` property (a string), '
                    'which would contain the URI of the next page in the list; 3) May itself have '
                    'a `forthcoming` property, for obtaining items that might be added to the '
                    'paginated list in the future. This can be a relative URI.',
        example='https://example.com/list?page=1',
    )
    totalItems = fields.Integer(
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description='An approximation for the total number of items in the paginated list. Will '
                    'not be present if the total number of items can not, or should not be '
                    'approximated.',
        example=123,
    )
    forthcoming = fields.String(
        dump_only=True,
        format='uri-reference',
        description='An URI for obtaining items that might be added to the paginated list in the '
                    'future. This is useful when we want to skip all items currently in the list, '
                    'but follow the forthcoming stream of new items. If this field is not '
                    'present, this means that the "streaming" feature is not supported by the '
                    'paginated list. The object retrieved from this URI will be of the same type '
                    'as the one retrieved from the `first` field. This can be a relative URI.',
        example='https://example.com/list?page=1000',
    )


class MessageSchema(Schema):
    type = fields.Function(
        lambda obj: 'Message',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Message',
    )
    messageId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format="uint64",
        description="The ID of this message. Later messages have bigger IDs.",
        example=12345,
    )
    creditorUri = fields.Function(
        lambda obj: endpoints.build_url('creditor', creditorId=obj.creditor_id),
        required=True,
        type='string',
        format="uri",
        description="The creditor's URI.",
        example='https://example.com/creditors/2/',
    )
    posted_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='postedAt',
        description='The moment at which this message was added to the log.',
    )


class MessagesPageSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/log',
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


class LinksPage(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/',
    )
    type = fields.Function(
        lambda obj: 'LinksPage',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='LinksPage',
    )
    items = fields.List(
        fields.String(format='uri-reference'),
        required=True,
        dump_only=True,
        description='An array of possibly relative URIs. Can be empty.',
        example=['1/', '11/', '111/'],
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=_PAGE_NEXT_DESCRIPTION.format(type='LinksPage'),
        example='?prev=111',
    )


class LedgerEntrySchema(Schema):
    entryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format="uint64",
        description="The ID of this entry. Later entries have bigger IDs.",
        example=123,
    )
    accountRecordUri = fields.Method(
        'get_account_record_uri',
        required=True,
        type='string',
        format="uri",
        description="The URI of the corresponding account record.",
        example='https://example.com/creditors/2/accounts/1/',
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format="int64",
        description="The amount added to account's principal. Can be positive (an increase) or "
                    "negative (a decrease). Can not be zero.",
        example=1000,
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format="int64",
        description='The new principal amount on the account, as it is after the transfer. Unless '
                    'a principal overflow has occurred, the new principal amount will be equal to '
                    '`amount` plus the old principal amount.',
        example=1500,
    )
    transferUri = fields.Method(
        'get_transfer_uri',
        required=True,
        type='string',
        format="uri",
        description='The URI of the corresponding transfer.',
        example='https://example.com/creditors/2/accounts/1/transfers/999',
    )
    posted_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='postedAt',
        description='The moment at which this entry was added to the ledger.',
    )
    previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        validate=validate.Range(min=1, max=MAX_UINT64),
        format="uint64",
        description="The ID of the previous entry in the account's ledger. Previous entries have "
                    "smaller IDs. When this field is not present, this means that there are no "
                    "previous entries.",
        example=122,
    )


class LedgerEntriesPage(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/entries?prev=124',
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