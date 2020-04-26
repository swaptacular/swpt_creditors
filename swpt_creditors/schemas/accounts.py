from marshmallow import Schema, fields, validate
from flask import url_for
from swpt_lib import endpoints
from .common import ObjectReferenceSchema, MAX_INT64, MAX_UINT64, URI_DESCRIPTION
from .paginated_lists import PaginatedListSchema

_DEBTOR_NAME_DESCRIPTION = '\
The name of the debtor. All account records belonging to a given \
creditor must have different `debtorName`s. The creditor may choose \
any name that is convenient, or easy to remember.'


class AccountCreationRequestSchema(Schema):
    debtor_uri = fields.Url(
        required=True,
        relative=False,
        schemes=[endpoints.get_url_scheme()],
        data_key='debtorUri',
        format='uri',
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    debtorName = fields.String(
        required=True,
        description=_DEBTOR_NAME_DESCRIPTION,
        example='Untied States of America',
    )


class AccountSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/debtors/1',
    )
    type = fields.Function(
        lambda obj: 'Account',
        required=True,
        dump_only=True,
        type='string',
        description="The type of this object. Different debtors may use different "
                    "schemas for the public information about their accounts. The "
                    "provided information must be enough to: 1) uniquely identify the "
                    "debtor, 2) uniquely identify the creditor's account with the "
                    "debtor. This field contains the name of the used schema.",
        example='Account',
    )
    creditor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The creditor's URI. This is useful when more information about the "
                    "owner of the account is needed.",
        example={'uri': '/creditors/2/'},
    )


class AccountRecordStatusSchema(Schema):
    type = fields.Function(
        lambda obj: 'AccountRecordStatus',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecordStatus',
    )


class AccountRecordConfigSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/config',
    )
    type = fields.Function(
        lambda obj: 'AccountRecordConfig',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecordConfig',
    )
    is_scheduled_for_deletion = fields.Boolean(
        missing=False,
        data_key='isScheduledForDeletion',
        description='Whether the account is scheduled for deletion. Most of the time, to safely '
                    'delete an account, it should be first scheduled for deletion, and deleted '
                    'only after the corresponding account record has been marked as safe for '
                    'deletion. If this field is not present, this means that this configuration '
                    'option is *not supported*.',
        example=False,
    )
    negligible_amount = fields.Float(
        missing=0.0,
        validate=validate.Range(min=0.0),
        data_key='negligibleAmount',
        description='The maximum amount that is considered negligible. It is used to '
                    'decide whether the account can be safely deleted, and whether a '
                    'transfer should be considered as insignificant. Must be '
                    'non-negative. If this field is not present, this means that this '
                    'configuration option is *not supported*.',
        example=0.0,
    )


class AccountRecordDisplaySettingsSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/display',
    )
    type = fields.Function(
        lambda obj: 'AccountRecordDisplaySettings',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecordDisplaySettings',
    )
    debtorUri = fields.Url(
        required=True,
        relative=False,
        schemes=[endpoints.get_url_scheme()],
        format='uri',
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    debtorName = fields.String(
        required=True,
        description=_DEBTOR_NAME_DESCRIPTION,
        example='Untied States of America',
    )
    hide = fields.Boolean(
        missing=False,
        description='If `true`, the account record will not be shown in the list of '
                    'account records belonging to the creditor. This may be convenient '
                    'for special-purpose accounts.',
        example=True,
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


class AccountRecordExchangeSettingsSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/exchange',
    )
    type = fields.Function(
        lambda obj: 'AccountRecordExchangeSettings',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecordExchangeSettings',
    )
    pegUri = fields.Url(
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        format='uri-reference',
        description="An URI of another account record, belonging to the same creditor, to "
                    "which the value of this account's tokens is pegged (via fixed exchange "
                    "rate). Can be a relative URI. This field is optional.",
        example='/creditors/2/accounts/11/',
    )
    fixedExchangeRate = fields.Float(
        missing=1.0,
        validate=validate.Range(min=0.0),
        description="The exchange rate between this account's tokens and \"pegUri\" account's "
                    "tokens. For example, `2.0` would mean that this account's tokens are "
                    "twice as valuable as \"pegUri\" account's tokens. (Note that this field "
                    "will be ignored if the `pegUri` field is not passed as well.)",
        example=1.0,
    )
    exchangeMode = fields.String(
        missing='off',
        description='The name of the active exchange mode. Different implementations may '
                    'define different exchange modes. `"off"` indicates that the account '
                    'must not participate in automatic exchanges.',
        example='conservative',
    )
    minPrincipal = fields.Integer(
        missing=-MAX_INT64,
        format='int64',
        description='The principal amount on the account should not fall below this value. '
                    '(Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases.)',
        example=1000,
    )
    maxPrincipal = fields.Integer(
        missing=0,
        format='int64',
        description='The principal amount on the account should not exceed this value. '
                    '(Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases.)',
        example=5000,
    )


class AccountRecordSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/',
    )
    type = fields.Function(
        lambda obj: 'AccountRecord',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecord',
    )
    portfolio = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the portfolio that contains this account record.",
        example={'uri': '/creditors/2/portfolio'},
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The account's URI. Uniquely identifies the account when it participates "
                    "in a transfer as sender or recipient.",
        example={'uri': '/creditors/2/debtors/1'},
    )
    created_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the account record was created.',
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format="int64",
        description='The principal amount on the account.',
        example=0,
    )
    interestRate = fields.Method(
        'get_interest_rate',
        dump_only=True,
        type='number',
        format="float",
        description='Annual rate (in percents) at which interest accumulates on the account. When '
                    'this field is not present, this means that the interest rate is unknown.',
        example=0.0,
    )
    ledgerEntries = fields.Nested(
        PaginatedListSchema,
        required=True,
        description='A paginated list of account ledger entries. That is: transfers for '
                    'which the account is either the sender or the recipient. The paginated '
                    'list will be sorted in reverse-chronological order (bigger entry IDs go '
                    'first). The entries will constitute a singly linked list, each entry '
                    '(except the most ancient one) referring to its ancestor.',
        example={
            'itemsType': 'LedgerEntry',
            'type': 'PaginatedList',
            'first': '/creditors/2/accounts/1/entries?prev=124',
        },
    )
    latestEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format="uint64",
        description="The ID of the latest entry in the account ledger.",
        example=123,
    )
    status = fields.Nested(
        AccountRecordStatusSchema,
        dump_only=True,
        required=True,
        description="Account status information.",
    )
    config = fields.Nested(
        AccountRecordConfigSchema,
        dump_only=True,
        required=True,
        description="The account's configuration. Can be changed by the owner of the account.",
    )
    displaySettings = fields.Nested(
        AccountRecordDisplaySettingsSchema,
        dump_only=True,
        required=True,
        description="The account's display settings. Can be changed by the owner of the account.",
    )
    exchangeSettings = fields.Nested(
        AccountRecordExchangeSettingsSchema,
        dump_only=True,
        required=True,
        description="The account's exchange settings. Can be changed by the owner of the account.",
    )
    is_deletion_safe = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isDeletionSafe',
        description='Whether it is safe to delete this account record. When `false`, deleting '
                    'the account record may result in losing a non-negligible amount of money '
                    'on the account.',
        example=False,
    )

    def get_uri(self, obj):
        return url_for(
            self.context['AccountRecord'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
