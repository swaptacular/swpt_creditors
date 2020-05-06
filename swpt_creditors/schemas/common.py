from marshmallow import Schema, fields, validate
from swpt_lib import endpoints

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1

URI_DESCRIPTION = '\
The URI of this object. Can be a relative URI.'

PAGE_NEXT_DESCRIPTION = '\
An URI of another `{type}` object which contains more items. When \
there are no remaining items, this field will not be present. If this field \
is present, there might be remaining items, even when the `items` array is \
empty. This can be a relative URI.'

PAGE_FORTHCOMING_DESCRIPTION = '\
An URI of another `{type}` object which would contain items that \
might be added in the future. That is: items that are not currently available, \
but may become available in the future. This is useful when we want to follow \
a continuous stream of new items. This field will not be present when the \
`next` field is present. This can be a relative URI.'

LATEST_UPDATE_AT_DESCRIPTION = '\
The moment of the latest update on this object.'


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
        format='uri-reference',
        description='The URI of the first page in the paginated list. The object retrieved from '
                    'this URI will have: 1) An `items` property (an array), which will contain the '
                    'first items of the paginated list; 2) May have a `next` property (a string), '
                    'which would contain the URI of the next page in the list; 3) May itself have '
                    'a `forthcoming` property, for obtaining items that might be added to the '
                    'paginated list in the future. This can be a relative URI.',
        example='/list?page=1',
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
        example='/list?page=1000',
    )


class ObjectReferenceSchema(Schema):
    uri = fields.Url(
        required=True,
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        format='uri-reference',
        description="The URI of the object. Can be a relative URI.",
        example='https://example.com/objects/1',
    )


class ObjectReferencesPageSchema(Schema):
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
        description='An array of `ObjectReference`s. Can be empty.',
        example=[{'uri': f'{i}/'} for i in [1, 11, 111]],
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='ObjectReferencesPage'),
        example='?prev=111',
    )


class AccountInfoSchema(Schema):
    type = fields.String(
        required=True,
        description="The type of this object. Different debtors may use different "
                    "**additional fields** containing information about the account. The "
                    "provided additional information must be sufficient to: 1) uniquely "
                    "and reliably identify the debtor, 2) uniquely and reliably identify "
                    "the creditor's account with the debtor. This field contains the "
                    "name of the used schema.",
        example='AccountInfo',
    )


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferError',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='TransferError',
    )
    errorCode = fields.String(
        required=True,
        dump_only=True,
        description='The error code.',
        example='INSUFFICIENT_AVAILABLE_AMOUNT',
    )
    avlAmount = fields.Integer(
        dump_only=True,
        format='int64',
        description='The amount currently available on the account.',
        example=10000,
    )


class TransferStatusSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferStatus',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='TransferStatus',
    )
    is_finalized = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='finalized',
        description='Whether the transfer has been finalized. A finalized transfer can be '
                    'either successful (no errors), or unsuccessful (one or more `errors`).',
        example=False,
    )
    finalized_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='finalizedAt',
        description='The moment at which the transfer has been finalized. If the transfer '
                    'has not been finalized yet, this field contains an estimation of when '
                    'the transfer will be finalized.',
    )
    errors = fields.Nested(
        TransferErrorSchema(many=True),
        missing=[],
        dump_only=True,
        description='Errors that have occurred during the execution of the transfer. If '
                    'the transfer has been completed successfully, this field will not '
                    'be present, or it will contain an empty array.',
    )
