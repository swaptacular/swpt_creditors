from marshmallow import Schema, fields, validate

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1

URI_DESCRIPTION = '\
The URI of this object. Can be a relative URI.'

UPDATE_ENTRY_ID_DESCRIPTION = '\
The ID of the latest `{type}` for this account in the log. It gets \
bigger after each update.'


class ObjectReferenceSchema(Schema):
    uri = fields.Url(
        required=True,
        format='uri-reference',
        description="The URI of the object. Can be a relative URI.",
        example='https://example.com/objects/1',
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


class LogEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LogEntry',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different kinds of log entries may use different '
                    '**additional fields**, containing more data. This field contains the name '
                    'of the used schema.',
        example='LogEntry',
    )
    entryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format="uint64",
        description="The ID of this log entry. Later log entries have bigger IDs.",
        example=12345,
    )
    posted_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='postedAt',
        description='The moment at which this entry was added to the log.',
    )


class LedgerEntrySchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'LedgerEntry',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntry',
    )
    ledger = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account ledger.",
        example={'uri': '/creditors/2/accounts/1/ledger'},
    )
    postedAmount = fields.Integer(
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
    transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the corresponding `CommittedTransfer`.',
        example={'uri': '/creditors/2/accounts/1/transfers/999'},
    )
    previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        validate=validate.Range(min=1, max=MAX_UINT64),
        format="uint64",
        description="The ID of the previous entry in the account's ledger. Previous entries have "
                    "smaller IDs. When this field is not present, this means that there are no "
                    "previous entries in the account's ledger.",
        example=122,
    )
