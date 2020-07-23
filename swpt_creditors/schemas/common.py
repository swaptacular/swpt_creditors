from datetime import datetime, timezone
from marshmallow import Schema, fields, validate, missing, validates, ValidationError

MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)

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


class ValidateTypeMixin:
    @validates('type')
    def validate_type(self, value):
        if f'{value}Schema' != type(self).__name__:
            raise ValidationError('Invalid type.')


class PaginationParametersSchema(Schema):
    prev = fields.String(
        description='Start with the item that follows the item with this index.',
        example='1',
    )
    stop = fields.String(
        description='Return only items which precedes the item with this index.',
        example='100',
    )


class PaginatedListSchema(Schema):
    type = fields.Function(
        lambda obj: 'PaginatedList',
        required=True,
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
        description='The URI of the first page in the paginated list. This can be a relative URI. '
                    'The object retrieved from this URI will have: 1) An `items` field (an '
                    'array), which will contain the first items of the paginated list; 2) May '
                    'have a `next` field (a string), which would contain the URI of the next '
                    'page in the list; 3) May have a `forthcoming` field, for obtaining items '
                    'that might be added to the paginated list in the future.',
        example='/list?page=1',
    )
    forthcoming = fields.String(
        dump_only=True,
        format='uri-reference',
        description='An optional URI for obtaining items that might be added to the paginated list '
                    'in the future. This is useful when we want to skip all items currently in the '
                    'list, but follow the forthcoming stream of new items. If this field is not '
                    'present, this means that the "streaming" feature is not supported by the '
                    'paginated list. The object retrieved from this URI will be of the same type as '
                    'the one retrieved from the `first` field. This can be a relative URI.',
        example='/list?page=1000',
    )


class ObjectReferenceSchema(Schema):
    uri = fields.Url(
        required=True,
        dump_only=True,
        relative=True,
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


class AccountIdentitySchema(Schema):
    uri = fields.String(
        required=True,
        validate=validate.Length(min=1, max=200),
        format='uri',
        description="The URI of the account. The information contained in the URI must be "
                    "enough to: 1) uniquely and reliably identify the debtor, 2) uniquely "
                    "and reliably identify the creditor's account with the debtor. Be aware "
                    "of the security implications if a network request need to be done in "
                    "order to identify the account.\n"
                    "\n"
                    "For example, if the debtor happens to be a bank, the URI would provide "
                    "the type of the debtor (a bank), the ID of the bank, and the bank "
                    "account number.",
    )


class MutableResourceSchema(Schema):
    latest_update_id = fields.Integer(
        required=True,
        dump_only=True,
        data_key='latestUpdateId',
        validate=validate.Range(min=1, max=MAX_INT64),
        format='int64',
        description='The ID of the latest `LogEntry` for this object in the log. This will be '
                    'a positive number, which gets bigger after each update.',
        example=123,
    )
    latest_update_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='latestUpdateAt',
        description='The moment of the latest update on this object. The value is the same as '
                    'the value of the `addedAt` field in the latest `LogEntry` for this object '
                    'in the log.',
    )
