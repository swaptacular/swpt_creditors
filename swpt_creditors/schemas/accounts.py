import re
import json
from copy import copy
from marshmallow import (
    Schema, fields, ValidationError, validate, validates_schema, pre_dump, post_dump,
    post_load, INCLUDE,
)
from swpt_lib.utils import i64_to_u64
from swpt_lib.swpt_uris import make_debtor_uri, make_account_uri
from swpt_creditors import models
from swpt_creditors.models import MIN_INT64, MAX_INT64, TS0
from .common import (
    ObjectReferenceSchema, AccountIdentitySchema, PaginatedListSchema,
    MutableResourceSchema, ValidateTypeMixin, URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION,
)

URLSAFE_B64 = re.compile(r'^[A-Za-z0-9_=-]*$')


class DebtorIdentitySchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorIdentity',
        default='DebtorIdentity',
        description='The type of this object.',
        example='DebtorIdentity',
    )
    uri = fields.String(
        required=True,
        validate=validate.Length(max=100),
        format='uri',
        description="The URI of the debtor. The information contained in the URI must be "
                    "enough to uniquely and reliably identify the debtor. Note that "
                    "a network request *should not be needed* to identify the account. "
                    "\n\n"
                    "For example, if the debtor happens to be a bank, the URI would reveal "
                    "the type of the debtor (a bank), and the ID of the bank. Note that "
                    "some debtors may be used only to represent a physical value measurement "
                    "unit (like ounces of gold). Those *dummy debtors* do not represent a "
                    "person or an organization, do not owe anything to anyone, and are used "
                    "solely as identifiers of value measurement units.",
        example='swpt:1',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        return obj


class DebtorInfoSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorInfo',
        default='DebtorInfo',
        description='The type of this object.',
        example='DebtorInfo',
    )
    url = fields.String(
        required=True,
        validate=validate.Length(max=200),
        format='uri',
        description='A link (Internationalized Resource Identifier) referring to a document '
                    'containing information about the debtor.',
        example='https://example.com/debtors/1/',
    )
    optional_content_type = fields.String(
        data_key='contentType',
        validate=validate.Length(max=100),
        description='Optional MIME type of the document that the `url` field refers to.',
        example='text/html',
    )
    optional_sha256 = fields.String(
        validate=validate.Regexp('^[0-9A-F]{64}$'),
        data_key='sha256',
        description='Optional SHA-256 cryptographic hash (Base16 encoded) of content of the '
                    'document that the `url` field refers to.',
        example='E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'url' in obj
        return obj


class CurrencyPegSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='CurrencyPeg',
        default='CurrencyPeg',
        description='The type of this object.',
        example='CurrencyPeg',
    )
    debtor = fields.Nested(
        DebtorIdentitySchema,
        required=True,
        description="The peg currency's `DebtorIdentity`.",
        example={'type': 'DebtorIdentity', 'uri': 'swpt:111'},
    )
    optional_debtor_home_url = fields.Url(
        validate=validate.Length(max=200),
        format='uri',
        data_key='debtorHomeUrl',
        description="An optional URL where the creditor can find sufficient information so as to "
                    "reliably identify the peg currency's debtor, and correctly configure an "
                    "account with it.",
        example='https://example.com/debtor-home-url',
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
    use_for_display = fields.Boolean(
        required=True,
        data_key='useForDisplay',
        description="Whether the peg dictates how the balance on the pegged account is displayed.",
    )
    display = fields.Nested(
        ObjectReferenceSchema,
        dump_only=True,
        description="The URI of the peg currency's `AccountDisplay` settings. When this field "
                    "is not present, this means that the creditor does not have an account in "
                    "the peg currency.",
        example={'uri': '/creditors/2/accounts/11/display'},
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'debtor' in obj
        assert 'exchangeRate' in obj
        assert 'useForDisplay' in obj
        return obj


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
        format='int64',
        data_key='entryId',
        description='The ID of the ledger entry. This will always be a positive number. The first '
                    'ledger entry has an ID of `1`, and the ID of each subsequent ledger entry will '
                    'be equal to the ID of the previous ledger entry plus one.',
        example=12345,
    )
    added_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='addedAt',
        description='The moment at which the entry was added to the ledger.',
    )
    aquired_amount = fields.Integer(
        required=True,
        dump_only=True,
        format='int64',
        data_key='aquiredAmount',
        description="The amount added to the account's principal. Can be a positive number (an "
                    "increase), a negative number (a decrease), or zero.",
        example=1000,
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        format='int64',
        description='The new principal amount on the account, as it is after the transfer. Unless '
                    'a principal overflow has occurred, the new principal amount will be equal to '
                    '`aquiredAmount` plus the old principal amount.',
        example=1500,
    )
    optional_transfer = fields.Nested(
        ObjectReferenceSchema,
        dump_only=True,
        data_key='transfer',
        description='Optional URI of the corresponding `CommittedTransfer`. When this field is '
                    'not present, this means that the ledger entry compensates for one or more '
                    'negligible transfers.',
        example={'uri': '/creditors/2/accounts/1/transfers/18444-999'},
    )

    @pre_dump
    def process_ledger_entry_instance(self, obj, many):
        assert isinstance(obj, models.LedgerEntry)
        paths = self.context['paths']
        obj = copy(obj)
        obj.ledger = {'uri': paths.account_ledger(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}
        if obj.creation_date is not None and obj.transfer_number is not None:
            obj.optional_transfer = {'uri': paths.committed_transfer(
                creditorId=obj.creditor_id,
                debtorId=obj.debtor_id,
                creationDate=obj.creation_date,
                transferNumber=obj.transfer_number,
            )}

        return obj


class LedgerEntriesPageSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
    next = fields.String(
        dump_only=True,
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='LedgerEntriesPage'),
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'items' in obj
        return obj


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
        format='int64',
        data_key='principal',
        description='The principal amount on the account.',
        example=0,
    )
    ledger_interest = fields.Integer(
        required=True,
        dump_only=True,
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
        dump_only=True,
        description='A `PaginatedList` of account `LedgerEntry`s. That is: transfers '
                    'for which the account is either the sender or the recipient. The '
                    'paginated list will be sorted in reverse-chronological order '
                    '(bigger `entryId`s go first).',
        example={
            'itemsType': 'LedgerEntry',
            'type': 'PaginatedList',
            'first': '/creditors/2/accounts/1/entries?prev=124',
        },
    )
    next_entry_id = fields.Integer(
        required=True,
        dump_only=True,
        format='int64',
        data_key='nextEntryId',
        description='The `entryID` of the next ledger entry to come. This will always be a '
                    'positive number. The first ledger entry for each account will have an ID '
                    'of `1`, and the ID of each subsequent ledger entry will be equal to the '
                    'ID of the previous ledger entry plus one.',
        example=123,
    )

    @pre_dump
    def process_account_data_instance(self, obj, many):
        assert isinstance(obj, models.AccountData)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_ledger(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}
        obj.latest_update_id = obj.ledger_latest_update_id
        obj.latest_update_ts = obj.ledger_latest_update_ts
        entries_path = paths.account_ledger_entries(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.next_entry_id = obj.ledger_last_entry_id + 1
        obj.entries = {
            'items_type': 'LedgerEntry',
            'first': f'{entries_path}?prev={obj.next_entry_id}'
        }

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
    optional_identity = fields.Nested(
        AccountIdentitySchema,
        dump_only=True,
        data_key='identity',
        description="Account's `AccountIdentity`. It uniquely and reliably identifies the "
                    "account when it participates in transfers as sender or recipient. When "
                    "this field is not present, this means that the account does not have "
                    "an identity yet (or anymore), and can not participate in transfers."
                    "\n\n"
                    "**Note:** This field will not be present at all for *dummy accounts*. "
                    "Dummy accounts can be useful for two purposes: 1) They can represent "
                    "physical value measurement units (like ounces of gold), to which "
                    "debtors can peg their currencies; 2) They can represent accounts with "
                    "debtors to which no network connection is available, still allowing "
                    "those accounts to act as links in a chain of currency pegs.",
        example={'type': 'AccountIdentity', 'uri': 'swpt:1/2'},
    )
    is_deletion_safe = fields.Boolean(
        required=True,
        dump_only=True,
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
    last_interest_rate_change_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='interestRateChangedAt',
        description='The moment at which the latest change in the interest rate happened.',
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
    optional_debtor_info = fields.Nested(
        DebtorInfoSchema,
        dump_only=True,
        data_key='debtorInfo',
        description='Optional information about the debtor.',
    )

    @pre_dump
    def process_account_data_instance(self, obj, many):
        assert isinstance(obj, models.AccountData)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_info(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}
        obj.latest_update_id = obj.info_latest_update_id
        obj.latest_update_ts = obj.info_latest_update_ts

        if obj.config_error is not None:
            obj.optional_config_error = obj.config_error

        if obj.debtor_info_url is not None:
            obj.optional_debtor_info = {'url': obj.debtor_info_url}

        try:
            obj.optional_identity = {'uri': make_account_uri(obj.debtor_id, obj.account_id)}
        except ValueError:
            pass

        return obj


class AccountKnowledgeSchema(ValidateTypeMixin, MutableResourceSchema):
    MAX_BYTES = 2000

    class Meta:
        unknown = INCLUDE

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
        example='AccountKnowledge',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    interestRate = fields.Float(
        description='Optional annual account interest rate (in percents), which is known to '
                    'the creditor.',
        example=0.0,
    )
    interestRateChangedAt = fields.DateTime(
        description='Optional moment at which the latest change in the interest rate has '
                    'happened, which is known to the creditor.',
    )
    identity = fields.Nested(
        AccountIdentitySchema,
        description="Optional `AccountIdentity`, which is known to the creditor.",
        example={'type': 'AccountIdentity', 'uri': 'swpt:1/2'},
    )
    debtorInfo = fields.Nested(
        DebtorInfoSchema,
        description='Optional `DebtorInfo`, which is known to the creditor.',
    )

    @validates_schema(pass_original=True)
    def validate_max_length(self, data, original_data, **kwargs):
        try:
            s = json.dumps(original_data, ensure_ascii=False, allow_nan=False)
        except ValueError:
            raise ValidationError("The message is not JSON compliant.")

        if len(s.encode('utf8')) > self.MAX_BYTES:
            raise ValidationError("The message is too big.")

    @pre_dump
    def process_account_knowledge_instance(self, obj, many):
        assert isinstance(obj, models.AccountKnowledge)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_knowledge(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}

        return obj

    @post_dump(pass_original=True)
    def include_data(self, obj, original_obj, many):
        result = obj
        if isinstance(original_obj.data, dict):
            result = {}
            result.update(original_obj.data)
            result.update(obj)

        return result

    @post_load(pass_original=True)
    def bundle_data(self, obj, original_obj, many, partial):
        for field in ['uri', 'type', 'account', 'latestUpdateId', 'latestUpdateAt']:
            original_obj.pop(field, None),

        return {
            'type': 'AccountKnowledge',
            'data': original_obj,
        }


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
        required=True,
        data_key='scheduledForDeletion',
        description='Whether the account is scheduled for deletion. The safest way to '
                    'delete an account whose status (`AccountInfo`) indicates that deletion '
                    'is not safe, is to first schedule it for deletion, and delete it only '
                    'when the account status indicates that deletion is safe. Note that '
                    'this may also require making outgoing transfers, so as to reduce the '
                    'balance on the account to a negligible amount.'
                    '\n\n'
                    '**Note:** For new accounts, the value of this field will be `False`.',
        example=False,
    )
    negligible_amount = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        data_key='negligibleAmount',
        description='The maximum amount that is considered negligible. It can be used '
                    'to decide whether the account can be safely deleted, and whether an '
                    'incoming transfer should be considered as insignificant. Must be '
                    'non-negative.'
                    '\n\n'
                    '**Note:** For new accounts, the value of this field will be some '
                    'huge number (`1e30` for example).',
        example=0.0,
    )
    allow_unsafe_deletion = fields.Boolean(
        required=True,
        data_key='allowUnsafeDeletion',
        description='Whether unsafe deletion of the account is allowed by the creditor. Note '
                    'that the deletion of an account which allows unsafe deletion may result in '
                    'losing a non-negligible amount of money on the account.'
                    '\n\n'
                    '**Note:** For new accounts, the value of this field will be `False`.',
        example=False,
    )

    @pre_dump
    def process_account_data_instance(self, obj, many):
        assert isinstance(obj, models.AccountData)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_config(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}
        obj.latest_update_id = obj.config_latest_update_id
        obj.latest_update_ts = obj.config_latest_update_ts

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
        example='AccountExchange',
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
        required=True,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        data_key='minPrincipal',
        description='The principal amount on the account should not fall below this value. '
                    'Note that this limit applies only for automatic exchanges, and is '
                    'enforced on "best effort" bases.',
        example=1000,
    )
    max_principal = fields.Integer(
        required=True,
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
            raise ValidationError("maxPrincipal must be equal or greater than minPrincipal.")

    @pre_dump
    def process_account_exchange_instance(self, obj, many):
        assert isinstance(obj, models.AccountExchange)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_exchange(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}
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
        example='AccountDisplay',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    amount_divisor = fields.Float(
        required=True,
        validate=validate.Range(min=0.0, min_inclusive=False),
        data_key='amountDivisor',
        description="Before displaying the amount, it should be divided by this number. For "
                    "new accounts the value of this field will be `1`."
                    "\n\n"
                    "**Important note:** This value should be used for display purposes "
                    "only. Notably, the value of this field must be ignored when the "
                    "exchange rate between pegged accounts is calculated.",

        example=100.0,
    )
    decimal_places = fields.Integer(
        required=True,
        validate=validate.Range(min=-20, max=20),
        data_key='decimalPlaces',
        description='The number of digits to show after the decimal point, when displaying '
                    'the amount. For new accounts the value of this field will be `0`.',
        example=2,
    )
    optional_debtor_name = fields.String(
        validate=validate.Length(min=1, max=40),
        data_key='debtorName',
        description='The name of the debtor. All accounts belonging to a given creditor '
                    'must have different `debtorName`s. The creditor may choose any name '
                    'that is convenient, or easy to remember.'
                    '\n\n'
                    "**Important note:** For new accounts this field will not be present, "
                    "and it must be set as soon as possible, otherwise the real identity "
                    "of the debtor may remain unknown to the creditor, which may lead "
                    "to confusion and financial loses.",
        example='United States of America',
    )
    optional_peg = fields.Nested(
        CurrencyPegSchema,
        data_key='peg',
        description="Optional `CurrencyPeg`, announced by the owner of the account. A "
                    "currency peg is a policy, in which the creditor sets a specific fixed "
                    "exchange rate between the tokens of two of his accounts (the pegged "
                    "currency, and the peg currency). Sometimes the peg currency is itself "
                    "pegged to another currency. This is called a \"peg-chain\".",
    )
    optional_unit = fields.String(
        validate=validate.Length(min=1, max=20),
        data_key='unit',
        description="The value measurement unit specified by the debtor. It should be "
                    "shown right after the displayed amount, \"500.00 USD\" for example. If "
                    "the account does not have its `unit` field set, the generic currency "
                    "sign (\u00a4), or the \"XXX\" ISO 4217 currency code should be shown."
                    "\n\n"
                    "To determine the value measurement unit in which to show the balance "
                    "on a given account, the account's \"peg-chain\" should be followed "
                    "until an account is found for which at least one of the following "
                    "conditions is true:"
                    "\n * The account is not pegged to another currency."
                    "\n * The account is pegged to another currency, but the "
                    "`CurrencyPeg` has its `useForDisplay` field set to `False`."
                    "\n * The account is pegged to another currency, but the "
                    "creditor does not have an account in this currency."
                    "\n * The account is pegged to another currency, the "
                    "creditor has an account in this currency, but the peg  "
                    "currency's account does not have its `unit` field set."
                    "\n\n"
                    "**Important note:** For new accounts this field will not be present, "
                    "and it must be set as soon as possible, otherwise the value measurement "
                    "unit may remain unknown to the creditor, which may lead to confusion "
                    "and financial loses.",
        example='USD',
    )
    hide = fields.Boolean(
        required=True,
        description="Whether the account should be hidden. That is: not shown when the user "
                    "views his account list. For new accounts the value of this field "
                    "will be `False`.",
        example=False,
    )

    @validates_schema
    def validate_debtor_name(self, data, **kwargs):
        if 'optional_debtor_name' in data:
            if 'optional_unit' not in data:
                raise ValidationError("Can not set debtorName without unit.")
        else:
            if 'optional_unit' in data:
                raise ValidationError("Can not set unit without debtorName.")
            if 'optional_peg' in data:
                raise ValidationError("Can not set peg without debtorName.")

    @pre_dump
    def process_account_display_instance(self, obj, many):
        assert isinstance(obj, models.AccountDisplay)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_display(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}

        if obj.unit is not None:
            obj.optional_unit = obj.unit

        if obj.debtor_name is not None:
            obj.optional_debtor_name = obj.debtor_name

        if obj.peg_exchange_rate is not None:
            peg = {
                'exchange_rate': obj.peg_exchange_rate,
                'debtor': {'uri': make_debtor_uri(obj.peg_currency_debtor_id)},
                'use_for_display': obj.peg_use_for_display,
            }
            if obj.peg_account_debtor_id is not None:
                display_path = paths.account_display(creditorId=obj.creditor_id, debtorId=obj.peg_account_debtor_id)
                peg['display'] = {'uri': display_path}
            if obj.peg_debtor_home_url is not None:
                peg['optional_debtor_home_url'] = obj.peg_debtor_home_url
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
        DebtorIdentitySchema,
        required=True,
        dump_only=True,
        data_key='debtor',
        description="Account's `DebtorIdentity`.",
        example={'type': 'DebtorIdentity', 'uri': 'swpt:1'},
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
        dump_only=True,
        description="Account's `AccountConfig` settings.",
    )
    display = fields.Nested(
        AccountDisplaySchema,
        required=True,
        dump_only=True,
        description="Account's `AccountDisplay` settings.",
    )
    exchange = fields.Nested(
        AccountExchangeSchema,
        required=True,
        dump_only=True,
        description="Account's `AccountExchange` settings.",
    )

    @pre_dump
    def process_account_instance(self, obj, many):
        assert isinstance(obj, models.Account)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)
        obj.debtor = {'uri': f'swpt:{i64_to_u64(obj.debtor_id)}'}
        obj.account_list = {'uri': paths.account_list(creditorId=obj.creditor_id)}
        obj.config = obj.data
        obj.info = obj.data
        obj.ledger = obj.data

        return obj


class AccountsPaginationParamsSchema(Schema):
    prev = fields.String(
        load_only=True,
        validate=validate.Regexp('^[0-9A-Za-z_=-]{1,64}$'),
        description='The returned fragment will begin with the first account that follows the '
                    'account whose debtor ID is equal to value of this parameter.',
        example='1',
    )


class LedgerEntriesPaginationParamsSchema(Schema):
    prev = fields.Integer(
        required=True,
        load_only=True,
        validate=validate.Range(min=0, max=MAX_INT64),
        format='int64',
        description='The returned fragment will begin with the latest ledger entry for the given '
                    'account, whose `entryId` is smaller (older) than the value of this parameter.',
        example=100,
    )
    stop = fields.Integer(
        missing=0,
        load_only=True,
        validate=validate.Range(min=0, max=MAX_INT64),
        format='int64',
        description='The returned fragment, and all the subsequent fragments, will contain only '
                    'ledger entries whose `entryId` is bigger (newer) than the value of this '
                    'parameter. This can be used to prevent repeatedly receiving ledger entries '
                    'that the client already knows about.',
        example=50,
    )
