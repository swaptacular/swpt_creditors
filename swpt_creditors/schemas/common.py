from marshmallow import Schema, fields, validate

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
URI_DESCRIPTION = 'The URI of this object. Can be a relative URI.'


class ObjectReferenceSchema(Schema):
    uri = fields.Url(
        required=True,
        format='uri-reference',
        description="The URI of the object. Can be a relative URI.",
        example='https://example.com/objects/1',
    )


class DisplaySettingsSchema(Schema):
    type = fields.Function(
        lambda obj: 'DisplaySettings',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='DisplaySettings',
    )
    debtorName = fields.String(
        required=True,
        description='The name of the debtor.',
        example='First Swaptacular Bank',
    )
    debtorUri = fields.Url(
        relative=False,
        format='uri',
        description='A link containing additional information about the debtor. This '
                    'field is optional.',
        example='https://example.com/debtors/1/',
    )
    amountDivisor = fields.Float(
        missing=1.0,
        validate=validate.Range(min=0.0, min_inclusive=False),
        description="The amount will be divided by this number before being displayed.",
        example=100.0,
    )
    decimalPlaces = fields.Integer(
        missing=0,
        description='The number of digits to show after the decimal point, when displaying '
                    'the amount.',
        example=2,
    )
    unitName = fields.String(
        description='The full name of the value measurement unit, "United States Dollars" '
                    'for example. This field is optional.',
        example='United States Dollars',
    )
    unitAbbr = fields.String(
        missing='\u00A4',
        description='A short abbreviation for the value measurement unit. It will be shown '
                    'right after the displayed amount, "500.00 USD" for example.',
        example='USD',
    )
    unitUri = fields.Url(
        relative=False,
        format='uri',
        description='A link containing additional information about the value measurement '
                    'unit. This field is optional.',
        example='https://example.com/units/USD',
    )
    hide = fields.Boolean(
        missing=False,
        description='If `true`, the account will not be shown in the list of '
                    'accounts belonging to the creditor. This may be convenient '
                    'for special-purpose accounts.',
        example=False,
    )


class DebtorInfoSchema(Schema):
    type = fields.String(
        required=True,
        description="The type of this object. Different debtor types may use different "
                    "schemas for the information about their accounts. The provided "
                    "information must be enough to uniquely and reliably identify the "
                    "debtor. This field contains the name of the used schema.",
        example='DebtorInfo',
    )
    displaySettings = fields.Nested(
        DisplaySettingsSchema,
        description='The account display settings recommended by the debtor. This field '
                    'is optional.',
    )


class AccountInfoSchema(Schema):
    type = fields.String(
        required=True,
        description="The type of this object. Different debtor types may use different "
                    "schemas for the information about their accounts. The provided "
                    "information must be enough to: 1) uniquely and reliably identify "
                    "the debtor, 2) uniquely reliably identify the creditor's account "
                    "with the debtor. This field contains the name of the used schema.",
        example='AccountInfo',
    )


class MessageSchema(Schema):
    type = fields.Function(
        lambda obj: 'Message',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different kinds of messages may use different '
                    'additional fields, containing more data. This field contains the name '
                    'of the used schema.',
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
    portfolio = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The creditor's portfolio URI.",
        example={'uri': '/creditors/2/portfolio'},
    )
    posted_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='postedAt',
        description='The moment at which this message was added to the log.',
    )


class LedgerEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LedgerEntry',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntry',
    )
    entryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format="uint64",
        description="The ID of this entry. Later entries have bigger IDs.",
        example=123,
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
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
        description='The URI of the corresponding transfer.',
        example={'uri': '/creditors/2/accounts/1/transfers/999'},
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
