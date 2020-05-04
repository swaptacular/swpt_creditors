from marshmallow import Schema, fields, validate
from flask import url_for
from .common import (
    ObjectReferenceSchema, AccountInfoSchema, PaginatedListSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION,
)

UPDATE_ENTRY_ID_DESCRIPTION = '\
The ID of the latest `{type}` entry for this account in the log. It \
gets bigger after each update.'


class AccountLedgerSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/ledger',
    )
    type = fields.Function(
        lambda obj: 'AccountLedger',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountLedger',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description='The principal amount on the account.',
        example=0,
    )
    entries = fields.Nested(
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
    latestLedgerEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description='The ID of the latest `LedgerEntry` for this account in the log.',
        example=123,
    )


class AccountStatusSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/status',
    )
    type = fields.Function(
        lambda obj: 'AccountStatus',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different debtors may use different '
                    '**additional fields**, providing more information about the status '
                    'of the account. This field contains the name of the used schema.',
        example='AccountStatus',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    is_deletion_safe = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isDeletionSafe',
        description='Whether it is safe to delete this account.',
        example=False,
    )
    interest_rate = fields.Float(
        dump_only=True,
        data_key='interestRate',
        description='Annual rate (in percents) at which interest accumulates on the account. When '
                    'this field is not present, this means that the interest rate is unknown.',
        example=0.0,
    )
    latestUpdateEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ENTRY_ID_DESCRIPTION.format(type='AccountStatusUpdate'),
        example=349,
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
    type = fields.String(
        missing='AccountConfig',
        description='The type of this object. Different debtors may use different '
                    '**additional fields**, providing more information about the '
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
        data_key='scheduledForDeletion',
        description='Whether the account is scheduled for deletion. The safest way to '
                    'delete an account which status indicates that deletion is not '
                    'safe, is to first schedule it for deletion, and delete it only '
                    'when the account status indicates that deletion is safe. Note '
                    'that this may also require making outgoing transfers, so as to '
                    'reduce the balance on the account to a negligible amount.',
        example=False,
    )
    negligible_amount = fields.Float(
        missing=0.0,
        validate=validate.Range(min=0.0),
        data_key='negligibleAmount',
        description='The maximum amount that is considered negligible. It can be used '
                    'to decide whether the account can be safely deleted, and whether an '
                    'incoming transfer should be considered as insignificant. Must be '
                    'non-negative.',
        example=0.0,
    )
    allow_unsafe_deletion = fields.Boolean(
        missing=False,
        data_key='allowUnsafeDeletion',
        description='Whether to allow unsafe deletion of the account. The deletion '
                    'of an account that allows unsafe deletion may result in losing a '
                    'non-negligible amount of money on the account.',
        example=True,
    )
    latestUpdateEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ENTRY_ID_DESCRIPTION.format(type='AccountConfigUpdate'),
        example=346,
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
    type = fields.String(
        missing='AccountExchangeSettings',
        description='The type of this object. Different implementations may use different '
                    '**additional fields**, providing more exchange settings for the '
                    'account. This field contains the name of the used schema.',
        example='AccountExchangeSettings',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    peg = fields.Nested(
        ObjectReferenceSchema,
        description="An optional URI of another account, belonging to the same creditor, "
                    "to which the value of this account's tokens is pegged (via the "
                    "defined `exchangeRate`).",
        example={'uri': '/creditors/2/accounts/11/'},
    )
    exchangeRate = fields.Float(
        validate=validate.Range(min=0.0),
        description="The exchange rate between this account's tokens and `peg`'s tokens. "
                    "For example, `2.0` would mean that this account's tokens are twice "
                    "as valuable as `peg`'s tokens. If `peg` is not set, the exchange "
                    "rate is between this account's tokens and some abstract universal "
                    "measure of value. (It does not really matter what this universal "
                    "measure of value is. Each creditor may choose the one that is most "
                    "convenient to him.) This field is optional.",
        example=1.0,
    )
    policy = fields.String(
        description='The name of the active automatic exchange policy. Different '
                    'implementations may define different exchange policies. This field is '
                    'optional. If it not present, this means that the account will not '
                    'participate in automatic exchanges.',
        example='conservative',
    )
    minPrincipal = fields.Integer(
        missing=-MAX_INT64,
        format='int64',
        description='The principal amount on the account should not fall below this value. '
                    'Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases.',
        example=1000,
    )
    maxPrincipal = fields.Integer(
        missing=MAX_INT64,
        format='int64',
        description='The principal amount on the account should not exceed this value. '
                    'Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases.',
        example=5000,
    )
    latestUpdateEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ENTRY_ID_DESCRIPTION.format(type='AccountExchangeSettingsUpdate'),
        example=347,
    )


class AccountDisplaySettingsSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/display',
    )
    type = fields.String(
        missing='AccountDisplaySettings',
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
        description='The name of the debtor. All accounts belonging to a given creditor '
                    'must have different `debtorName`s. The creditor may choose any '
                    'name that is convenient, or easy to remember.',
        example='First Swaptacular Bank',
    )
    debtorUrl = fields.Url(
        relative=False,
        format='uri',
        description='An optional link containing additional information about the debtor.',
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
        description='Optional full name of the value measurement unit, "United States '
                    'Dollars" for example.',
        example='United States Dollars',
    )
    unitAbbr = fields.String(
        missing='\u00A4',
        description='A short abbreviation for the value measurement unit. It will be shown '
                    'right after the displayed amount, "500.00 USD" for example.',
        example='USD',
    )
    unitUrl = fields.Url(
        relative=False,
        format='uri',
        description='An optional link containing additional information about the value '
                    'measurement unit.',
        example='https://example.com/units/USD',
    )
    hide = fields.Boolean(
        missing=False,
        description='If `true`, the account will not be shown in the list of '
                    'accounts belonging to the creditor. This may be convenient '
                    'for special-purpose accounts.',
        example=False,
    )
    latestUpdateEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ENTRY_ID_DESCRIPTION.format(type='AccountDisplaySettingsUpdate'),
        example=348,
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


class AccountCreationRequestSchema(Schema):
    type = fields.String(
        missing='AccountCreationRequest',
        description='The type of this object.',
        example='AccountCreationRequest',
    )
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
        description="Optional account exchange settings.",
    )
    config = fields.Nested(
        AccountConfigSchema,
        description="Optional account configuration.",
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
    ledger = fields.Nested(
        AccountLedgerSchema,
        required=True,
        dump_only=True,
        description="Account ledger information.",
    )
    status = fields.Nested(
        AccountStatusSchema,
        required=True,
        dump_only=True,
        description="Account status information.",
    )
    config = fields.Nested(
        AccountConfigSchema,
        required=True,
        dump_only=True,
        description="The account's configuration.",
    )
    displaySettings = fields.Nested(
        AccountDisplaySettingsSchema,
        required=True,
        dump_only=True,
        description="The account's display settings.",
    )
    exchangeSettings = fields.Nested(
        AccountExchangeSettingsSchema,
        required=True,
        dump_only=True,
        description="The account's exchange settings.",
    )
    latestUpdateEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ENTRY_ID_DESCRIPTION.format(type='AccountUpdate'),
        example=344,
    )

    def get_uri(self, obj):
        return url_for(
            self.context['Account'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
