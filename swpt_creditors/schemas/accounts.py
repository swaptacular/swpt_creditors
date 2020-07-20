import re
from base64 import urlsafe_b64encode
from copy import copy
from marshmallow import Schema, ValidationError, fields, validate, pre_dump, validates_schema
from flask import url_for
from swpt_lib.utils import i64_to_u64
from swpt_creditors import models
from .common import (
    ObjectReferenceSchema, AccountIdentitySchema, PaginatedListSchema,
    MutableResourceSchema, ValidateTypeMixin,
    MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, BEGINNING_OF_TIME,
    URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION,
)

URLSAFE_B64 = re.compile(r'^[A-Za-z0-9_=-]*$')


class DebtorSchema(Schema):
    uri = fields.String(
        required=True,
        validate=validate.Length(min=1, max=100),
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


class CurrencyPegSchema(ValidateTypeMixin, Schema):
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
        data_key='exchangeRate',
        description="The exchange rate between the pegged currency and the peg currency. For "
                    "example, `2.0` would mean that pegged currency's tokens are twice as "
                    "valuable as peg currency's tokens.",
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
    uri = fields.String(
        required=True,
        dump_only=True,
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
    ledger_principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        data_key='principal',
        description='The principal amount on the account.',
        example=0,
    )
    ledger_interest = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        data_key='interest',
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

    @pre_dump
    def process_account_data_instance(self, obj, many):
        assert not many
        assert isinstance(obj, models.AccountData)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['AccountLedger'],
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
        obj.latest_update_id = obj.ledger_latest_update_id
        obj.latest_update_ts = obj.ledger_latest_update_ts
        obj.ledger_principal = obj.ledger_principal
        obj.ledger_interest = obj.interest  # TODO: calculate the interest precisely.
        obj.entries = {}  # TODO: pass a paginated list here.

        return obj


class AccountInfoSchema(MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
        required=True,
        dump_only=True,
        data_key='safeToDelete',
        description='Whether it is safe to delete this account.',
        example=False,
    )
    interest_rate = fields.Float(
        missing=0.0,
        dump_only=True,
        data_key='interestRate',
        description='Annual rate (in percents) at which interest accumulates on the account.',
        example=0.0,
    )
    last_interest_rate_change_ts = fields.DateTime(
        missing=BEGINNING_OF_TIME,
        dump_only=True,
        data_key='interestRateChangedAt',
        description='The moment at which the latest change in the interest rate happened.',
    )
    overflown = fields.Boolean(
        dump_only=True,
        missing=False,
        description='Whether the account is "overflown". A `true` indicates that the account\'s '
                    'principal have breached the `int64` boundaries.',
        example=False,
    )
    optional_config_error = fields.String(
        dump_only=True,
        data_key='configError',
        description='When this field is present, this means that for some reason, the current '
                    '`AccountConfig` settings can not be applied, or are not effectual anymore. '
                    'Usually this means that there has been a network communication problem, or a '
                    'system configuration problem. The value alludes to the cause of the problem.',
        example='CONFIG_IS_INEFFECTUAL',
    )
    optional_debtor_url = fields.String(
        dump_only=True,
        format='uri',
        data_key='debtorUrl',
        description='Optional link containing additional information about the debtor.',
        example='https://example.com/debtors/1/',
    )

    @pre_dump
    def process_account_data_instance(self, obj, many):
        assert not many
        assert isinstance(obj, models.AccountData)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['AccountInfo'],
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
        obj.latest_update_id = obj.info_latest_update_id
        obj.latest_update_ts = obj.info_latest_update_ts

        if obj.config_error is not None:
            obj.optional_config_error = obj.config_error

        if obj.debtor_url is not None:
            obj.optional_debtor_url = obj.debtor_url

        account_identity = obj.account_identity
        if account_identity:
            # TODO: Use a `swpt_lib.utils` function for this.
            if not URLSAFE_B64.match(account_identity):
                base64encoded = urlsafe_b64encode(account_identity.encode('utf8'))
                account_identity = f'!{base64encoded.decode()}'

            obj.identity = {'uri': f'swpt:{i64_to_u64(obj.debtor_id)}/{account_identity}'}

        return obj


class AccountKnowledgeSchema(ValidateTypeMixin, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
    optional_identity = fields.Nested(
        AccountIdentitySchema,
        data_key='identity',
        description="An `AccountIdentity`, which is known to the creditor.",
        example={'uri': 'swpt:1/2'},
    )
    optional_debtor_url = fields.String(
        validate=validate.Length(min=1, max=100),
        format='uri',
        data_key='debtorUrl',
        description='A link for additional information about the debtor, which is known to '
                    'the creditor.',
        example='https://example.com/debtors/1/',
    )
    optional_currency_peg = fields.Nested(
        CurrencyPegSchema,
        data_key='currencyPeg',
        description='A `CurrencyPeg` announced by the debtor, which is known to the creditor.',
    )

    @pre_dump
    def process_account_knowledge_instance(self, obj, many):
        assert not many
        assert isinstance(obj, models.AccountKnowledge)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['AccountKnowledge'],
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

        if obj.debtor_url is not None:
            obj.optional_debtor_url = obj.debtor_url

        if obj.peg_exchange_rate is not None:
            obj.optional_currency_peg = {
                'exchange_rate': obj.peg_exchange_rate,
                'debtor': {'uri': obj.peg_debtor_uri},
            }

        if obj.identity_uri is not None:
            obj.optional_identity = {'uri': obj.identity_uri}

        return obj


class AccountConfigSchema(ValidateTypeMixin, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
        required=True,
        data_key='scheduledForDeletion',
        description='Whether the account is scheduled for deletion. The safest way to '
                    'delete an account whose status (`AccountInfo`) indicates that deletion '
                    'is not safe, is to first schedule it for deletion, and delete it only '
                    'when the account status indicates that deletion is safe. Note that '
                    'this may also require making outgoing transfers, so as to reduce the '
                    'balance on the account to a negligible amount.',
        example=False,
    )
    negligible_amount = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        data_key='negligibleAmount',
        description='The maximum amount that is considered negligible. It can be used '
                    'to decide whether the account can be safely deleted, and whether an '
                    'incoming transfer should be considered as insignificant. Must be '
                    'non-negative. **For new accounts, the value of this field will be '
                    'a huge number** (`1e30` for example).',
        example=0.0,
    )
    config = fields.String(
        missing='',
        validate=validate.Length(max=400),
        description='Additional account configuration settings. Different debtors may '
                    'use different formats for this field.',
        example='',
    )
    allow_unsafe_deletion = fields.Boolean(
        missing=False,
        data_key='allowUnsafeDeletion',
        description='Whether unsafe deletion of the account is allowed by the creditor. Note '
                    'that the deletion of an account which allows unsafe deletion may result in '
                    'losing a non-negligible amount of money on the account.',
        example=False,
    )

    @pre_dump
    def process_account_config_instance(self, obj, many):
        assert not many
        assert isinstance(obj, models.AccountConfig)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['AccountConfig'],
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

        return obj


class AccountExchangeSchema(ValidateTypeMixin, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
    optional_policy = fields.String(
        validate=validate.Length(min=1, max=40),
        data_key='policy',
        description='The name of the active automatic exchange policy. Different '
                    'implementations may define different exchange policies. This field is '
                    'optional. If it not present, this means that the account will not '
                    'participate in automatic exchanges.',
        example='conservative',
    )
    min_principal = fields.Integer(
        missing=MIN_INT64,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        data_key='minPrincipal',
        description='The principal amount on the account should not fall below this value. '
                    'Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases.',
        example=1000,
    )
    max_principal = fields.Integer(
        missing=MAX_INT64,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        data_key='maxPrincipal',
        description='The principal amount on the account should not exceed this value. '
                    'Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases. The value of `maxPrincipal` must '
                    'be equal or greater than the value of `minPrincipal`',
        example=5000,
    )

    @validates_schema
    def validate_max_principal(self, data, **kwargs):
        if data['min_principal'] > data['max_principal']:
            raise ValidationError("max_principal must be equal or greater than min_principal")

    @pre_dump
    def process_account_exchange_instance(self, obj, many):
        assert not many
        assert isinstance(obj, models.AccountExchange)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['AccountExchange'],
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
        if obj.policy is not None:
            obj.optional_policy = obj.policy

        return obj


class AccountDisplaySchema(ValidateTypeMixin, MutableResourceSchema):
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
    amount_divisor = fields.Float(
        missing=1.0,
        validate=validate.Range(min=0.0, min_inclusive=False),
        data_key='amountDivisor',
        description="Account's amounts should be divided by this number before being "
                    "displayed. Important note: This value should be used for display "
                    "purposes only. Notably, the value of this field must be ignored when "
                    "the exchange rate between pegged accounts is being calculated.",
        example=100.0,
    )
    decimal_places = fields.Integer(
        missing=0,
        validate=validate.Range(min=-20, max=20),
        data_key='decimalPlaces',
        description='The number of digits to show after the decimal point, when displaying '
                    'the amount.',
        example=2,
    )
    optional_debtor_name = fields.String(
        validate=validate.Length(min=1, max=40),
        data_key='debtorName',
        description='The name of the debtor. **All accounts belonging to a given '
                    'creditor must have different `debtorName`s. When a new account '
                    'has been created, this field will not be present, and it must '
                    'be set as soon as possible**, otherwise the real identity of the '
                    'debtor may remain unknown to the creditor, which may lead to '
                    'confusion and financial loses. The creditor may choose any '
                    'name that is convenient, or easy to remember.',
        example='United States of America',
    )
    optional_peg = fields.Nested(
        AccountPegSchema,
        data_key='peg',
        description="Optional `AccountPeg`, announced by the owner of the account. An "
                    "account peg is a policy, in which the creditor sets a specific fixed "
                    "exchange rate between the tokens of two of his accounts (the pegged "
                    "currency, and the peg currency). Sometimes the peg currency is itself "
                    "pegged to another currency. This is called a \"peg-chain\".",
    )
    optional_own_unit = fields.String(
        validate=validate.Length(min=1, max=4),
        data_key='ownUnit',
        description="Optional abbreviation for a value measurement unit that is unique for the "
                    "account's debtor. It should be shown right after the displayed amount, "
                    "\"500.00 USD\" for example. **All accounts belonging to a given creditor must "
                    "have different `ownUnit`s**. Thus, setting this field for an account is most "
                    "probably a bad idea, unless the account's debtor tokens are already widely "
                    "recognized. Notably, one currency being pegged to another currency is not "
                    "a good reason for the pegged currency to have the same `ownUnit` as the peg "
                    "currency. In practice, many of creditor's accounts might be pegged to other "
                    "accounts, and only a few would need to have their `ownUnit` field set.",
        example='USD',
    )
    own_unit_preference = fields.Integer(
        missing=0,
        validate=validate.Range(min=MIN_INT32, max=MAX_INT32),
        data_key='ownUnitPreference',
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
        assert isinstance(obj, models.AccountDisplay)
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

        if obj.own_unit is not None:
            obj.optional_own_unit = obj.own_unit

        if obj.debtor_name is not None:
            obj.optional_debtor_name = obj.debtor_name

        if obj.peg_exchange_rate is not None:
            peg = {
                'exchange_rate': obj.peg_exchange_rate,
                'debtor': {'uri': obj.peg_debtor_uri},
            }
            if obj.peg_debtor_id is not None:
                peg['display'] = {'uri': url_for(
                    self.context['AccountDisplay'],
                    _external=False,
                    creditorId=obj.creditor_id,
                    debtorId=obj.peg_debtor_id,
                )}
            obj.optional_peg = peg

        return obj


class AccountSchema(MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
    account_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='accountList',
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

    @pre_dump
    def process_account_instance(self, obj, many):
        assert not many
        assert isinstance(obj, models.Account)
        obj = copy(obj)
        obj.uri = url_for(
            self.context['Account'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
        obj.debtor = {'uri': f'swpt:{i64_to_u64(obj.debtor_id)}'}
        obj.account_list = {'uri': url_for(
            self.context['AccountList'],
            _external=False,
            creditorId=obj.creditor_id,
        )}
        obj.info = obj.data
        obj.ledger = obj.data

        return obj
