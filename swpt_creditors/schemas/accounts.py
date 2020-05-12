from marshmallow import Schema, fields, validate
from flask import url_for
from .common import (
    ObjectReferenceSchema, AccountIdentitySchema, PaginatedListSchema,
    MIN_INT32, MAX_INT32, MAX_INT64, MAX_UINT64, URI_DESCRIPTION, LATEST_UPDATE_AT_DESCRIPTION,
)

UPDATE_ID_DESCRIPTION = '\
The ID of the latest `{type}` entry for this account in the log. It \
gets bigger after each update.'


class DebtorSchema(Schema):
    type = fields.String(
        required=True,
        description="The type of this object. Different debtors may use different "
                    "**additional fields** containing information about the debtor. The "
                    "provided information must be just enough to uniquely and reliably "
                    "identify the debtor. This field contains the name of the used schema.",
        example='Debtor',
    )


class CurrencyPegSchema(Schema):
    type = fields.Function(
        lambda obj: 'CurrencyPeg',
        required=True,
        type='string',
        description='The type of this object.',
        example='CurrencyPeg',
    )
    debtor = fields.Nested(
        DebtorSchema,
        required=True,
        description="The peg currency's `Debtor`.",
        example={'type': 'SwptDebtor', 'debtorId': 111},
    )
    exchangeRate = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        description="The exchange rate between the pegged currency and the peg currency. For "
                    "example, `2.0` would mean that pegged currency's tokens are twice as "
                    "valuable as peg currency's tokens.",
        example=1.0,
    )


class AccountPegSchema(Schema):
    type = fields.String(
        missing='AccountPeg',
        description='The type of this object.',
        example='AccountPeg',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        description='The URI of the `Account` which acts as a peg currency.',
        example={'uri': '/creditors/2/accounts/11/'},
    )
    exchangeRate = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        description="The exchange rate between the pegged currency and the peg currency. For "
                    "example, `2.0` would mean that pegged currency's tokens are twice as "
                    "valuable as peg currency's tokens.",
        example=1.0,
    )


class DisplaySchema(Schema):
    type = fields.Function(
        lambda obj: 'Display',
        required=True,
        type='string',
        description='The type of this object.',
        example='Display',
    )
    debtorName = fields.String(
        required=True,
        description='The name of the debtor.',
        example='United States of America',
    )
    amountDivisor = fields.Float(
        missing=1.0,
        validate=validate.Range(min=0.0, min_inclusive=False),
        description="Account's amounts should be divided by this number before being "
                    "displayed. Important note: This value should be used for display "
                    "purposes only. Notably, the value of this field must be ignored when "
                    "the exchange rate between pegged accounts is being calculated.",
        example=100.0,
    )
    decimalPlaces = fields.Integer(
        missing=0,
        description='The number of digits to show after the decimal point, when displaying '
                    'the amount.',
        example=2,
    )
    ownUnit = fields.String(
        description="Optional abbreviation for a value measurement unit that is unique for the "
                    "account's debtor. It should be shown right after the displayed amount, "
                    "\"500.00 USD\" for example. All accounts belonging to a given creditor must "
                    "have different `ownUnit`s. Thus, setting this field for an account is most "
                    "probably a bad idea, unless the account's debtor tokens are already widely "
                    "recognized. Notably, one currency being pegged to another currency is not "
                    "a good reason for the pegged currency to have the same `ownUnit` as the peg "
                    "currency. In practice, many of creditor's accounts might be pegged to other "
                    "accounts, and only a few would need to have their `ownUnit` field set.",
        example='USD',
    )


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
        type='string',
        description='The type of this object.',
        example='AccountLedger',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
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
        description='A `PaginatedList` of account `LedgerEntry`s. That is: transfers for '
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
        format='uint64',
        description='The ID of the latest `LedgerEntry` for this account in the log.',
        example=123,
    )
    latestEntryAt = fields.DateTime(
        required=True,
        dump_only=True,
        description='The moment of the latest update on this object. The value is the same as '
                    'the value of the `postedAt` field of the latest `LedgerEntry` for this '
                    'account in the log.',
    )


class AccountInfoSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/info',
    )
    type = fields.Function(
        lambda obj: 'AccountInfo',
        required=True,
        type='string',
        description='The type of this object. Different debtors may use different '
                    '**additional fields**, providing more information about the '
                    'account. This field contains the name of the used schema.',
        example='AccountInfo',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    identity = fields.Nested(
        AccountIdentitySchema,
        dump_only=True,
        description="An `AccountIdentity` object, containing information that uniquely and "
                    "reliably identifies the account when it participates in transfers as "
                    "sender or recipient. For example, if the debtor happens to be a bank, "
                    "this would contain the type of the debtor (a bank), the ID of the "
                    "bank, and the bank account number. When this field is not present, "
                    "this means that the account has not obtained identity yet, and can "
                    "not participate in transfers.",
        example={'type': 'SwptAccount', 'debtorId': 1, 'creditorId': 2},
    )
    is_deletion_safe = fields.Boolean(
        dump_only=True,
        missing=False,
        data_key='safeToDelete',
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
    misconfigured = fields.Boolean(
        dump_only=True,
        missing=False,
        description='Whether the account is misconfigured. A `true` means that, for some reason, '
                    'the current `AccountConfig` can not be applied, or is not effectual anymore. '
                    'Usually this means that there has been some kind of network communication '
                    'or system configuration problem.',
        example=False,
    )
    dummy = fields.Boolean(
        missing=False,
        description="Whether the account is a *dummy account*. Dummy accounts are accounts whose "
                    "balances are always zero, and no transfers can be made from/to them. Dummy "
                    "accounts can be useful for two purposes: 1) They can represent physical "
                    "value measurement units (like ounces of gold), to which debtors can peg "
                    "their currencies; 2) They can represent accounts with debtors to which no "
                    "network connection is available, still allowing those accounts to act as "
                    "links in a chain of currency pegs. Dummy accounts might be displayed "
                    "differently from normal accounts, or not displayed at all.",
        example=False,
    )
    currencyPeg = fields.Nested(
        CurrencyPegSchema,
        dump_only=True,
        description="Optional `CurrencyPeg`, announced by the debtor. A currency peg is a policy "
                    "in which the debtor sets a specific fixed exchange rate for its currency "
                    "with other debtor's currency (the peg currency).",
    )
    debtorUrl = fields.Url(
        dump_only=True,
        relative=False,
        format='uri',
        description='Optional link containing additional information about the debtor.',
        example='https://example.com/debtors/1/',
    )
    recommendedDisplay = fields.Nested(
        DisplaySchema,
        dump_only=True,
        description='Optional recommended `Display` settings.',
    )
    latestUpdateId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ID_DESCRIPTION.format(type='AccountInfoUpdate'),
        example=349,
    )
    latestUpdateAt = fields.DateTime(
        required=True,
        dump_only=True,
        description=LATEST_UPDATE_AT_DESCRIPTION.format(type='AccountInfoUpdate'),
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
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    is_scheduled_for_deletion = fields.Boolean(
        missing=False,
        data_key='scheduledForDeletion',
        description='Whether the account is scheduled for deletion. The safest way to '
                    'delete an account whose status (`AccountInfo`) indicates that deletion '
                    'is not safe, is to first schedule it for deletion, and delete it only '
                    'when the account status indicates that deletion is safe. Note that'
                    'this may also require making outgoing transfers, so as to reduce the '
                    'balance on the account to a negligible amount.',
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
        example=False,
    )
    latestUpdateId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ID_DESCRIPTION.format(type='AccountConfigUpdate'),
        example=346,
    )
    latestUpdateAt = fields.DateTime(
        required=True,
        dump_only=True,
        description=LATEST_UPDATE_AT_DESCRIPTION.format(type='AccountConfigUpdate'),
    )


class AccountExchangeSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/exchange',
    )
    type = fields.String(
        missing='AccountExchange',
        description='The type of this object. Different implementations may use different '
                    '**additional fields**, providing more exchange settings for the '
                    'account. This field contains the name of the used schema.',
        example='AccountExchange',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
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
    latestUpdateId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ID_DESCRIPTION.format(type='AccountExchangeUpdate'),
        example=347,
    )
    latestUpdateAt = fields.DateTime(
        required=True,
        dump_only=True,
        description=LATEST_UPDATE_AT_DESCRIPTION.format(type='AccountExchangeUpdate'),
    )


class AccountDisplaySchema(DisplaySchema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/display',
    )
    type = fields.String(
        missing='AccountDisplay',
        description='The type of this object.',
        example='AccountDisplay',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    debtorName = fields.String(
        description='The name of the debtor. All accounts belonging to a given creditor '
                    'must have different `debtorName`s. The creditor may choose any '
                    'name that is convenient, or easy to remember. Initially (when a '
                    'new account is created) this field will not be set, and **it should '
                    'be set as soon as possible**, otherwise the real identity of the '
                    'debtor may remain unknown to the creditor, which may lead to '
                    'confusion and financial loses.',
        example='United States of America',
    )
    peg = fields.Nested(
        AccountPegSchema,
        description="Optional `AccountPeg`, announced by the owner of the account. An "
                    "account peg is a policy, in which the creditor sets a specific fixed "
                    "exchange rate between the tokens of two of his accounts (the pegged "
                    "currency, and the peg currency).",
    )
    ownUnitPreference = fields.Integer(
        missing=0,
        validate=validate.Range(min=MIN_INT32, max=MAX_INT32),
        format='int32',
        description="A number that expresses creditor's preference for seeing the balances on "
                    "other accounts, measured in this account's `ownUnit`. A bigger number "
                    "indicates a bigger preference (negative numbers are allowed too). To "
                    "determine the value measurement unit in which to show the balance on a given "
                    "account, the account's `peg`-chain should be followed (skipping accounts "
                    "without `ownUnit`), and the unit with the biggest `ownUnitPreference` "
                    "value should be chosen. In case of a tie, units that are closer down the "
                    "chain of pegs should be preferred. If no unit is found, the generic currency "
                    "sign (\u00a4), or the \"XXX\" ISO 4217 currency code should be shown.",
        example=0,
    )
    latestUpdateId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ID_DESCRIPTION.format(type='AccountDisplayUpdate'),
        example=348,
    )
    latestUpdateAt = fields.DateTime(
        required=True,
        dump_only=True,
        description=LATEST_UPDATE_AT_DESCRIPTION.format(type='AccountDisplayUpdate'),
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
        type='string',
        description='The type of this object.',
        example='Account',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet` that contains this account.",
        example={'uri': '/creditors/2/wallet'},
    )
    debtor = fields.Nested(
        DebtorSchema,
        required=True,
        description="A `Debtor` object, containing information that uniquely and "
                    "reliably identifies the debtor. For example, if the debtor happens "
                    "to be a  bank, this would contain the type of the debtor (a bank), "
                    "and the ID of the bank. Note that some accounts may represent a "
                    "physical value measurement unit (like ounces of gold), and be "
                    "useful only as links in a chain of currency pegs. Those *dummy "
                    "accounts*  will have *dummy debtors*, which do not represent a "
                    "person or an organization, do not owe anything to anyone, and are "
                    "used solely as identifiers of the value measurement unit.",
        example={'type': 'SwptDebtor', 'debtorId': 1},
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
        description="Account's `AccountLedger`.",
    )
    info = fields.Nested(
        AccountInfoSchema,
        required=True,
        dump_only=True,
        description="Account's `AccountInfo`.",
    )
    config = fields.Nested(
        AccountConfigSchema,
        required=True,
        description="Account's `AccountConfig`.",
    )
    display = fields.Nested(
        AccountDisplaySchema,
        required=True,
        description="Account's `AccountDisplay` settings.",
    )
    exchange = fields.Nested(
        AccountExchangeSchema,
        required=True,
        description="Account's `AccountExchange` settings.",
    )
    latestUpdateId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description=UPDATE_ID_DESCRIPTION.format(type='AccountUpdate'),
        example=344,
    )
    latestUpdateAt = fields.DateTime(
        required=True,
        dump_only=True,
        description=LATEST_UPDATE_AT_DESCRIPTION.format(type='AccountUpdate'),
    )

    def get_uri(self, obj):
        return url_for(
            self.context['Account'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
