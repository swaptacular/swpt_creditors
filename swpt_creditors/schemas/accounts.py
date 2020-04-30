from marshmallow import Schema, fields, validate
from flask import url_for
from swpt_lib import endpoints
from .common import (
    ObjectReferenceSchema, AccountInfoSchema, PaginatedListSchema, MessageSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION, REVISION_DESCRIPTION,
)

_DEBTOR_NAME_DESCRIPTION = '\
The name of the debtor. All accounts belonging to a given \
creditor must have different `debtorName`s. The creditor may choose \
any name that is convenient, or easy to remember.'


class AccountStatusSchema(Schema):
    type = fields.Function(
        lambda obj: 'AccountStatus',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different debtors may use different '
                    '**additional fields**, containing more information about the status '
                    'of the account. This field contains the name of the used schema.',
        example='AccountStatus',
    )
    is_deletion_safe = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isDeletionSafe',
        description='Whether it is safe to delete this account. When `false`, deleting '
                    'the account may result in losing a non-negligible amount of money '
                    'on the account.',
        example=False,
    )
    interest_rate = fields.Float(
        dump_only=True,
        data_key='interestRate',
        description='Annual rate (in percents) at which interest accumulates on the account. When '
                    'this field is not present, this means that the interest rate is unknown.',
        example=0.0,
    )


class AccountConfigSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/config',
    )
    type = fields.Function(
        lambda obj: 'AccountConfig',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different debtors may use different '
                    '**additional fields**, containing more information about the '
                    'configuration of the account. This field contains the name '
                    'of the used schema.',
        example='AccountConfig',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
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
    revision = fields.Integer(
        required=True,
        dump_only=True,
        format='uint64',
        description=REVISION_DESCRIPTION,
        example=0,
    )


class AccountExchangeSettingsSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/exchange',
    )
    type = fields.Function(
        lambda obj: 'AccountExchangeSettings',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountExchangeSettings',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    pegUri = fields.Url(
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        format='uri-reference',
        description="An URI of another account, belonging to the same creditor, to "
                    "which the value of this account's tokens is pegged (via fixed "
                    "exchange rate). Can be a relative URI. This field is optional.",
        example='/creditors/2/accounts/11/',
    )
    fixedExchangeRate = fields.Float(
        missing=1.0,
        validate=validate.Range(min=0.0),
        description="The exchange rate between this account's tokens and \"pegUri\" account's "
                    "tokens. For example, `2.0` would mean that this account's tokens are "
                    "twice as valuable as \"pegUri\" account's tokens. Note that this field "
                    "will be ignored if the `pegUri` field has not been passed.",
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
    revision = fields.Integer(
        required=True,
        dump_only=True,
        format='uint64',
        description=REVISION_DESCRIPTION,
        example=0,
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


class AccountDisplaySettingsSchema(DisplaySettingsSchema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/display',
    )
    type = fields.Function(
        lambda obj: 'AccountDisplaySettings',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountDisplaySettings',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    debtorName = fields.String(
        required=True,
        description=_DEBTOR_NAME_DESCRIPTION,
        example='First Swaptacular Bank',
    )
    revision = fields.Integer(
        required=True,
        dump_only=True,
        format='uint64',
        description=REVISION_DESCRIPTION,
        example=0,
    )


class DebtorInfoSchema(Schema):
    type = fields.String(
        required=True,
        description="The type of this object. Different debtors may use different "
                    "**additional fields** containing information about the debtor. The "
                    "provided information must be sufficient to uniquely and reliably "
                    "identify the debtor. This field contains the name of the used schema.",
        example='DebtorInfo',
    )
    displaySettings = fields.Nested(
        DisplaySettingsSchema,
        description='The account display settings recommended by the debtor. This field '
                    'is optional.',
    )


class AccountCreationRequestSchema(Schema):
    debtorInfo = fields.Nested(
        DebtorInfoSchema,
        required=True,
        description="A JSON object containing information that uniquely and reliably "
                    "identifies the debtor. For example, if the debtor happens to be a "
                    "bank, this would contain the type of the debtor (a bank), and the "
                    "ID of the bank.",
        example={'type': 'SwptDebtorInfo', 'debtorId': 1},
    )
    displaySettings = fields.Nested(
        AccountDisplaySettingsSchema,
        required=True,
        description="Account's display settings.",
    )
    exchangeSettings = fields.Nested(
        AccountExchangeSettingsSchema,
        description="Account's exchange settings.",
    )
    config = fields.Nested(
        AccountConfigSchema,
        description="Account's configuration.",
    )


class AccountSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/',
    )
    type = fields.Function(
        lambda obj: 'Account',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Account',
    )
    portfolio = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's portfolio that contains this account.",
        example={'uri': '/creditors/2/portfolio'},
    )
    accountInfo = fields.Nested(
        AccountInfoSchema,
        required=True,
        dump_only=True,
        description="A JSON object containing information that uniquely and reliably "
                    "identifies the creditor's account when it participates in transfers "
                    "as sender or recipient. For example, if the debtor happens to be a "
                    "bank, this would contain the type of the debtor (a bank), the ID of "
                    "the bank, and the bank account number.",
        example={'type': 'SwptAccountInfo', 'debtorId': 1, 'creditorId': 2},
    )
    created_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the account was created.',
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format="int64",
        description='The principal amount on the account.',
        example=0,
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
        AccountStatusSchema,
        dump_only=True,
        required=True,
        description="Account status information.",
    )
    config = fields.Nested(
        AccountConfigSchema,
        dump_only=True,
        required=True,
        description="The account's configuration.",
    )
    displaySettings = fields.Nested(
        AccountDisplaySettingsSchema,
        dump_only=True,
        required=True,
        description="The account's display settings.",
    )
    exchangeSettings = fields.Nested(
        AccountExchangeSettingsSchema,
        dump_only=True,
        required=True,
        description="The account's exchange settings.",
    )

    def get_uri(self, obj):
        return url_for(
            self.context['Account'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )


class AccountChangeMessageSchema(MessageSchema):
    type = fields.Function(
        lambda obj: 'AccountChangeMessage',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountChangeMessage',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the changed account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    isDeleted = fields.Boolean(
        dump_only=True,
        missing=False,
        description="Whether the account has been deleted.",
    )
    changedConfig = fields.Integer(
        dump_only=True,
        format='uint64',
        description="The new config revision number. Will not be present if "
                    "the account is deleted or newly created.",
        example=1,
    )
    changedDisplaySettings = fields.Integer(
        dump_only=True,
        format='uint64',
        description="The new display settings revision number. Will not be "
                    "present if the account is deleted or newly created.",
        example=1,
    )
    changedExchangeSettings = fields.Integer(
        dump_only=True,
        format='uint64',
        description="The new exchange settings revision number. Will not be "
                    "present if the account is deleted or newly created.",
        example=1,
    )
    changedStatus = fields.Nested(
        AccountStatusSchema,
        dump_only=True,
        description="The new account status information. Will not be present "
                    "if the account is deleted or newly created.",
    )
