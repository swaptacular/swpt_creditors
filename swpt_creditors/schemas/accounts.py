from copy import copy
from marshmallow import Schema, fields, validate, pre_dump, post_load, missing
from flask import url_for
from swpt_creditors.models import AccountDisplay
from .common import (
    ObjectReferenceSchema, AccountIdentitySchema, PaginatedListSchema, MutableResourceSchema,
    MIN_INT32, MAX_INT32, MAX_INT64, URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION, BEGINNING_OF_TIME
)


class DebtorSchema(Schema):
    uri = fields.String(
        required=True,
        format='uri',
        description="The URI of the debtor. The information contained in the URI must be "
                    "enough to uniquely and reliably identify the debtor. Be aware of the "
                    "security implications if a network request need to be done in order "
                    "to identify the debtor.\n"
                    "\n"
                    "For example, if the debtor happens to be a bank, the URI would provide "
                    "the type of the debtor (a bank), and the ID of the bank. Note that "
                    "some debtors may be used only to represent a physical value measurement "
                    "unit (like ounces of gold). Those *dummy debtors* do not represent a "
                    "person or an organization, do not owe anything to anyone, and are used "
                    "solely as identifiers of value measurement units.",
        example='swpt:1',
    )


class CurrencyPegSchema(Schema):
    type = fields.String(
        missing='CurrencyPeg',
        default='CurrencyPeg',
        description='The type of this object.',
    )
    debtor = fields.Nested(
        DebtorSchema,
        required=True,
        description="The peg currency's `Debtor`.",
        example={'uri': 'swpt:111'},
    )
    exchange_rate = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        description="The exchange rate between the pegged currency and the peg currency. For "
                    "example, `2.0` would mean that pegged currency's tokens are twice as "
                    "valuable as peg currency's tokens.",
        data_key='exchangeRate',
        example=1.0,
    )


class AccountPegSchema(CurrencyPegSchema):
    type = fields.String(
        missing='AccountPeg',
        default='AccountPeg',
        description='The type of this object.',
    )
    display = fields.Nested(
        ObjectReferenceSchema,
        dump_only=True,
        description="The URI of the peg currency's `AccountDisplay` settings. When this field "
                    "is not present, this means that the creditor does not have an account in "
                    "the peg currency.",
        example={'uri': '/creditors/2/accounts/11/display'},
    )


class LedgerEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LedgerEntry',
        required=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntry',
    )
    ledger = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the corresponding `AccountLedger`.',
        example={'uri': '/creditors/2/accounts/1/ledger'},
    )
    entry_id = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format='int64',
        data_key='entryId',
        description='The ID of the ledger entry. Later ledger entries have bigger IDs. Note '
                    'that those IDs are the same as the IDs of the `LogEntry`s added to '
                    'the log to inform about the change in the corresponding `AccountLedger`.',
        example=12345,
    )
    previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        validate=validate.Range(min=1, max=MAX_INT64),
        format='int64',
        description="The `entryId` of the previous `LedgerEntry` for this account. Previous "
                    "entries have smaller IDs. When this field is not present, this means "
                    "that there are no previous entries in the account's ledger.",
        example=122,
    )
    added_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='addedAt',
        description='The moment at which the entry was added to the ledger.',
    )
    aquiredAmount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description="The amount added to the account's principal. Can be a positive number (an "
                    "increase), or a negative number (a decrease). Can not be zero.",
        example=1000,
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description='The new principal amount on the account, as it is after the transfer. Unless '
                    'a principal overflow has occurred, the new principal amount will be equal to '
                    '`aquiredAmount` plus the old principal amount.',
        example=1500,
    )
    transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the corresponding `CommittedTransfer`.',
        example={'uri': '/creditors/2/accounts/1/transfers/18444/999'},
    )


class LedgerEntriesPageSchema(Schema):
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
        type='string',
        description='The type of this object.',
        example='LedgerEntriesPage',
    )
    items = fields.Nested(
        LedgerEntrySchema(many=True),
        required=True,
        dump_only=True,
        description='An array of `LedgerEntry`s. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='LedgerEntriesPage'),
    )


class AccountLedgerSchema(MutableResourceSchema):
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
    interest = fields.Method(
        'get_interest',
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        type='integer',
        format='int64',
        description='The approximate amount of interest accumulated on the account, which '
                    'has not been added to the principal yet. This can be a negative number. '
                    'Once in a while, the accumulated interest will be zeroed out and added '
                    'to the principal (an interest payment).'
                    '\n\n'
                    '**Note:** The value of this field is calculated on-the-fly, so it may '
                    'change from one request to another, and no `LogEntry` for the change '
                    'will be added to the log.',
        example=0,
    )
    entries = fields.Nested(
        PaginatedListSchema,
        required=True,
        description='A `PaginatedList` of account `LedgerEntry`s. That is: transfers '
                    'for which the account is either the sender or the recipient. The '
                    'paginated list will be sorted in reverse-chronological order '
                    '(bigger `entryId`s go first). The entries will constitute a singly '
                    'linked list, each entry (except the most ancient one) referring to '
                    'its ancestor.',
        example={
            'itemsType': 'LedgerEntry',
            'type': 'PaginatedList',
            'first': '/creditors/2/accounts/1/entries?prev=124',
        },
    )

    def get_interest(self, obj):
        return 0


class AccountInfoSchema(MutableResourceSchema):
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
        description='The type of this object.',
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
        description="Account's `AccountIdentity`. It uniquely and reliably identifies the "
                    "account when it participates in transfers as sender or recipient. When "
                    "this field is not present, this means that the account has not "
                    "obtained identity yet, and can not participate in transfers.\n"
                    "\n"
                    "Note that some accounts may be used only to represent a physical value "
                    "measurement unit (like ounces of gold), and are useful only as links in "
                    "a chain of currency pegs. Those *dummy accounts* will have *dummy debtors*, "
                    "which do not represent a person or an organization, do not owe anything "
                    "to anyone, and are used solely as identifiers of value measurement "
                    "units. For dummy accounts, this field will never be present.",
        example={'uri': 'swpt:1/2'},
    )
    is_deletion_safe = fields.Boolean(
        dump_only=True,
        missing=False,
        data_key='safeToDelete',
        description='Whether it is safe to delete this account.',
        example=False,
    )
    interest_rate = fields.Float(
        required=True,
        dump_only=True,
        data_key='interestRate',
        description='Annual rate (in percents) at which interest accumulates on the account.',
        example=0.0,
    )
    interest_rate_changed_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='interestRateChangedAt',
        description='The moment at which the latest change in the interest rate happened.',
    )
    configError = fields.String(
        dump_only=True,
        description='When this field is present, this means that for some reason, the current '
                    '`AccountConfig` settings can not be applied, or are not effectual anymore. '
                    'Usually this means that there has been a network communication problem, or a '
                    'system configuration problem. The value alludes to the cause of the problem.',
        example='CONFIG_IS_INEFFECTUAL',
    )
    unreachable = fields.Boolean(
        dump_only=True,
        missing=False,
        description='Whether the account is "unreachable". A `true` indicates that the account '
                    'can not receive incoming transfers.',
        example=False,
    )
    overflown = fields.Boolean(
        dump_only=True,
        missing=False,
        description='Whether the account is "overflown". A `true` indicates that the account\'s '
                    'principal have breached the `int64` boundaries.',
        example=False,
    )
    debtorUrl = fields.String(
        dump_only=True,
        format='uri',
        description='Optional link containing additional information about the debtor.',
        example='https://example.com/debtors/1/',
    )


class AccountKnowledgeSchema(MutableResourceSchema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/knowledge',
    )
    type = fields.String(
        missing='AccountKnowledge',
        default='AccountKnowledge',
        description='The type of this object.',
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
        description="An `AccountIdentity`, which is known to the creditor.",
        example={'uri': 'swpt:1/2'},
    )
    interest_rate = fields.Float(
        missing=0.0,
        data_key='interestRate',
        description='An annual account interest rate (in percents), which is known to the creditor.',
        example=0.0,
    )
    interest_rate_changed_at_ts = fields.DateTime(
        missing=BEGINNING_OF_TIME,
        data_key='interestRateChangedAt',
        description='The moment at which the latest change in the interest rate, which is known '
                    'to the creditor, has happened.',
    )
    debtorUrl = fields.String(
        format='uri',
        description='A link for additional information about the debtor, which is known to '
                    'the creditor.',
        example='https://example.com/debtors/1/',
    )
    currencyPeg = fields.Nested(
        CurrencyPegSchema,
        description='A `CurrencyPeg` announced by the debtor, which is known to the creditor.',
    )
    allow_unsafe_deletion = fields.Boolean(
        missing=False,
        data_key='allowUnsafeDeletion',
        description='Whether unsafe deletion of the account is allowed by the creditor. Note '
                    'that the deletion of an account which allows unsafe deletion may result in '
                    'losing a non-negligible amount of money on the account.',
        example=False,
    )


class AccountConfigSchema(MutableResourceSchema):
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
        default='AccountConfig',
        description='The type of this object.',
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
    config = fields.String(
        missing='',
        description='Additional account configuration settings. Different debtors may '
                    'use different formats for this field.',
        example='',
    )


class AccountExchangeSchema(MutableResourceSchema):
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
        default='AccountExchange',
        description='The type of this object. Different implementations may use different '
                    '**additional fields**, providing more exchange settings for the '
                    'account. This field contains the name of the used schema.',
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


class AccountDisplaySchema(MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/display',
    )
    type = fields.String(
        missing='AccountDisplay',
        default='AccountDisplay',
        description='The type of this object.',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    debtor_name = fields.String(
        description='The name of the debtor. **All accounts belonging to a given '
                    'creditor must have different `debtorName`s. When a new account '
                    'has been created, this field will not be present, and it must '
                    'be set as soon as possible**, otherwise the real identity of the '
                    'debtor may remain unknown to the creditor, which may lead to '
                    'confusion and financial loses. The creditor may choose any '
                    'name that is convenient, or easy to remember.',
        data_key='debtorName',
        example='United States of America',
    )
    peg = fields.Nested(
        AccountPegSchema,
        description="Optional `AccountPeg`, announced by the owner of the account. An "
                    "account peg is a policy, in which the creditor sets a specific fixed "
                    "exchange rate between the tokens of two of his accounts (the pegged "
                    "currency, and the peg currency). Sometimes the peg currency is itself "
                    "pegged to another currency. This is called a \"peg-chain\".",
    )
    amount_divisor = fields.Float(
        missing=1.0,
        validate=validate.Range(min=0.0, min_inclusive=False),
        description="Account's amounts should be divided by this number before being "
                    "displayed. Important note: This value should be used for display "
                    "purposes only. Notably, the value of this field must be ignored when "
                    "the exchange rate between pegged accounts is being calculated.",
        data_key='amountDivisor',
        example=100.0,
    )
    decimal_places = fields.Integer(
        missing=0,
        description='The number of digits to show after the decimal point, when displaying '
                    'the amount.',
        data_key='decimalPlaces',
        example=2,
    )
    own_unit = fields.String(
        description="Optional abbreviation for a value measurement unit that is unique for the "
                    "account's debtor. It should be shown right after the displayed amount, "
                    "\"500.00 USD\" for example. **All accounts belonging to a given creditor must "
                    "have different `ownUnit`s**. Thus, setting this field for an account is most "
                    "probably a bad idea, unless the account's debtor tokens are already widely "
                    "recognized. Notably, one currency being pegged to another currency is not "
                    "a good reason for the pegged currency to have the same `ownUnit` as the peg "
                    "currency. In practice, many of creditor's accounts might be pegged to other "
                    "accounts, and only a few would need to have their `ownUnit` field set.",
        data_key='ownUnit',
        example='USD',
    )
    own_unit_preference = fields.Integer(
        missing=0,
        validate=validate.Range(min=MIN_INT32, max=MAX_INT32),
        format='int32',
        data_key='ownUnitPreference',
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
    hide = fields.Boolean(
        missing=False,
        description="If `true`, the account should not be shown in the list of accounts "
                    "belonging to the creditor. This may be convenient for special-purpose "
                    "accounts. For example, *dummy accounts* are accounts whose balances "
                    "are always zero, and no transfers can be made from/to them. Dummy "
                    "accounts can be useful for two purposes: 1) They can represent physical "
                    "value measurement units (like ounces of gold), to which debtors can peg "
                    "their currencies; 2) They can represent accounts with debtors to which no "
                    "network connection is available, still allowing those accounts to act as "
                    "links in a chain of currency pegs.",
        example=False,
    )

    @pre_dump
    def process_account_display_instance(self, obj, many):
        assert not many
        assert isinstance(obj, AccountDisplay)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['AccountDisplay'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
        obj.account = {'uri': url_for(
            self.context['Account'],
            _external=False,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )}

        if obj.peg_exchange_rate is None:
            obj.peg = missing
        else:
            if obj.peg_debtor_id is None:
                display = missing
            else:
                display = {'uri': url_for(
                    self.context['AccountDisplay'],
                    _external=False,
                    creditorId=obj.creditor_id,
                    debtorId=obj.peg_debtor_id,
                )}
            obj.peg = {
                'exchange_rate': obj.peg_exchange_rate,
                'debtor': {'uri': obj.peg_debtor_uri},
                'display': display,
            }

        if obj.own_unit is None:
            obj.own_unit = missing

        if obj.debtor_name is None:
            obj.debtor_name = missing

        return obj

    @post_load
    def refine_object(self, obj, many, **kwargs):
        assert not many

        if 'own_unit' not in obj:
            obj['own_unit'] = None

        if 'debtor_name' not in obj:
            obj['debtor_name'] = None

        if 'peg' not in obj:
            obj['peg_exchange_rate'] = None
            obj['peg_debtor_uri'] = None
        else:
            peg = obj['peg']
            del obj['peg']
            obj['peg_exchange_rate'] = peg['exchange_rate']
            obj['peg_debtor_uri'] = peg['debtor']['uri']

        return obj


class AccountSchema(MutableResourceSchema):
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
    accountList = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of creditor's `AccountList`.",
        example={'uri': '/creditors/2/account-list'},
    )
    debtor = fields.Nested(
        DebtorSchema,
        required=True,
        description="Account's `Debtor`.",
        example={'uri': 'swpt:1'},
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
    knowledge = fields.Nested(
        AccountKnowledgeSchema,
        required=True,
        dump_only=True,
        description="Account's `AccountKnowledge` settings.",
    )
    config = fields.Nested(
        AccountConfigSchema,
        required=True,
        description="Account's `AccountConfig` settings.",
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

    def get_uri(self, obj):
        return url_for(
            self.context['Account'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
