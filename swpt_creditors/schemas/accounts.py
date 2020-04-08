from marshmallow import Schema, fields, validate
from flask import url_for
from swpt_lib import endpoints
from .common import MAX_INT64, MAX_UINT64
from .paginated_lists import PaginatedListSchema


class AccountCreationRequestSchema(Schema):
    debtor_uri = fields.Url(
        required=True,
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        data_key='debtorUri',
        format='uri',
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )


class AccountSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/debtors/1',
    )
    type = fields.Function(
        lambda obj: 'Account',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Account',
    )
    debtorUri = fields.Function(
        lambda obj: endpoints.build_url('debtor', debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    creditorUri = fields.Function(
        lambda obj: endpoints.build_url('creditor', creditorId=obj.creditor_id),
        required=True,
        type='string',
        format="uri",
        description="The creditor's URI.",
        example='https://example.com/creditors/2/',
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
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/config',
    )
    type = fields.Function(
        lambda obj: 'AccountRecordConfig',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecordConfig',
    )
    accountRecordUri = fields.Method(
        'get_account_record_uri',
        required=True,
        type='string',
        format="uri",
        description="The URI of the corresponding account record.",
        example='https://example.com/creditors/2/accounts/1/',
    )
    changed_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='changedAt',
        description='The moment at which the last change in the account configuration was made.',
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


class AccountRecordSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/',
    )
    type = fields.Function(
        lambda obj: 'AccountRecord',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecord',
    )
    portfolioUri = fields.Method(
        'get_portfolio_uri',
        required=True,
        type='string',
        format="uri",
        description="The URI of the portfolio that contains this account record.",
        example='https://example.com/creditors/2/portfolio',
    )
    accountUri = fields.Function(
        lambda obj: endpoints.build_url('account', creditorId=obj.creditor_id, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The account's URI. Uniquely identifies the account when it participates "
                    "in a transfer as sender or recipient.",
        example='https://example.com/creditors/2/debtors/1',
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
            "itemsType": "LedgerEntry",
            "type": "PaginatedList",
            "first": "https://example.com/creditors/2/accounts/1/entries?first=123",
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
