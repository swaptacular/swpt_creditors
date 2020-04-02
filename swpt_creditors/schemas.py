from datetime import datetime, timezone, timedelta
from typing import NamedTuple, List
from marshmallow import Schema, fields, validate
from flask import url_for, current_app
from .models import MAX_INT64, Creditor, InitiatedTransfer
from swpt_lib import endpoints


class TransfersCollection(NamedTuple):
    creditor_id: int
    items: List[str]


class CreditorCreationOptionsSchema(Schema):
    pass


class PaginationParametersSchema(Schema):
    first = fields.Integer(
        format='uint64',
        validate=validate.Range(min=0, max=(1 << 64) - 1),
        description='Return only items that have equal or greater index than this value.',
        example=0,
    )


class PaginatedListSchema(Schema):
    type = fields.Constant(
        'PaginatedList',
        dump_only=True,
        type='string',
        description='The type of this object.',
    )
    itemsType = fields.Method(
        'get_items_type',
        required=True,
        type='string',
        description='The type of the items in the paginated list.',
        example='string',
    )
    first = fields.Method(
        'get_first',
        required=True,
        type='string',
        format="uri",
        description='The URI of the first page in the paginated list. The object retrieved from '
                    'this URI will have an `items` property (an array), which will contain the '
                    'first items of the paginated list. The retrieved object may also have a '
                    '`next` property (a string), which would contain the URI of the next page '
                    'in the paginated list.',
        example='https://example.com/list?page=1',
    )
    totalItems = fields.Method(
        'get_length',
        dump_only=True,
        type='integer',
        format='uint64',
        description='An approximation for the total number of items in the paginated list. Will '
                    'not be present if the total number of items can not, or should not be '
                    'approximated.',
        example=123,
    )


class RelativeLinksPage(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/?first=0',
    )
    type = fields.Constant(
        'RelativeLinksPage',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='RelativeLinksPage',
    )
    items = fields.List(
        fields.String(format='uri-reference'),
        required=True,
        dump_only=True,
        description='An array of possibly relative URIs. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description='An URI of another `RelativeLinksPage` object which contains more items. When '
                    'there are no remaining items, this field will not be present. If this field '
                    'is present, there might be remaining items, even when the `items` array is '
                    'empty. This can be a relative URI.',
        example='?first=1234567890',
    )


class CreditorSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/1',
    )
    type = fields.Constant(
        'Creditor',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Creditor',
    )
    created_at_date = fields.Date(
        required=True,
        dump_only=True,
        data_key='createdOn',
        description=Creditor.created_at_date.comment,
        example='2019-11-30',
    )
    is_active = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isActive',
        description="Whether the creditor is currently active or not."
    )

    def get_uri(self, obj):
        return url_for(self.context['Creditor'], _external=True, creditorId=obj.creditor_id)


class TransferErrorSchema(Schema):
    errorCode = fields.String(
        required=True,
        dump_only=True,
        description='The error code.',
        example='INSUFFICIENT_AVAILABLE_AMOUNT',
    )
    avlAmount = fields.Integer(
        required=False,
        dump_only=True,
        format="int64",
        description='The amount currently available on the account.',
        example=10000,
    )


class DirectTransferCreationRequestSchema(Schema):
    transfer_uuid = fields.UUID(
        required=True,
        data_key='transferUuid',
        description="A client-generated UUID for the transfer.",
        example='123e4567-e89b-12d3-a456-426655440000',
    )
    debtor_uri = fields.Url(
        required=True,
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        data_key='debtorUri',
        format='uri',
        description="The debtor's URI.",
        example='https://example.com/debtors/1',
    )
    recipient_uri = fields.Url(
        required=True,
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        data_key='recipientUri',
        format='uri',
        description="The recipient's URI.",
        example='https://example.com/creditors/1111',
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=InitiatedTransfer.amount.comment,
        example=1000,
    )
    transfer_info = fields.Dict(
        missing={},
        data_key='transferInfo',
        description=InitiatedTransfer.transfer_info.comment,
    )


class TransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/1/transfers/123e4567-e89b-12d3-a456-426655440000',
    )
    type = fields.Constant(
        'Transfer',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Transfer',
    )
    debtor_uri = fields.String(
        required=True,
        dump_only=True,
        data_key='debtorUri',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/debtors/1',
    )
    senderUri = fields.Function(
        lambda obj: endpoints.build_url('creditor', creditorId=obj.creditor_id),
        required=True,
        type='string',
        format="uri",
        description="The sender's URI.",
        example='https://example.com/creditors/1',
    )
    recipient_uri = fields.String(
        required=True,
        dump_only=True,
        data_key='recipientUri',
        format="uri",
        description="The recipient's URI.",
        example='https://example.com/creditors/1111',
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=InitiatedTransfer.amount.comment,
        example=1000,
    )
    transfer_info = fields.Dict(
        required=True,
        dump_only=True,
        data_key='transferInfo',
        description=InitiatedTransfer.transfer_info.comment,
    )
    initiated_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='initiatedAt',
        description=InitiatedTransfer.initiated_at_ts.comment,
    )
    is_finalized = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isFinalized',
        description='Whether the transfer has been finalized or not.',
        example=True,
    )
    finalizedAt = fields.Method(
        'get_finalized_at_string',
        required=True,
        type='string',
        format='date-time',
        description='The moment at which the transfer has been finalized. If the transfer '
                    'has not been finalized yet, this field contains an estimation of when '
                    'the transfer should be finalized.',
    )
    is_successful = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isSuccessful',
        description=InitiatedTransfer.is_successful.comment,
        example=False,
    )
    errors = fields.Nested(
        TransferErrorSchema(many=True),
        dump_only=True,
        required=True,
        description='Errors that occurred during the transfer.'
    )

    def get_uri(self, obj):
        return url_for(
            self.context['Transfer'],
            _external=True,
            creditorId=obj.creditor_id,
            transferUuid=obj.transfer_uuid,
        )

    def get_finalized_at_string(self, obj):
        if obj.is_finalized:
            finalized_at_ts = obj.finalized_at_ts
        else:
            current_ts = datetime.now(tz=timezone.utc)
            current_delay = current_ts - obj.initiated_at_ts
            average_delay = timedelta(seconds=current_app.config['APP_TRANSFERS_FINALIZATION_AVG_SECONDS'])
            finalized_at_ts = current_ts + max(current_delay, average_delay)
        return finalized_at_ts.isoformat()


class TransfersCollectionSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/1/transfers/',
    )
    type = fields.Constant(
        'TransfersCollection',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='TransfersCollection',
    )
    creditorUri = fields.Function(
        lambda obj: endpoints.build_url('creditor', creditorId=obj.creditor_id),
        required=True,
        type='string',
        format="uri",
        description="The creditor's URI.",
        example='https://example.com/creditor/1',
    )
    totalItems = fields.Function(
        lambda obj: len(obj.items),
        required=True,
        type='integer',
        description="The number of items in the `items` array.",
        example=2,
    )
    items = fields.List(
        fields.String(format='uri-reference'),
        required=True,
        dump_only=True,
        description="An unordered set of *relative* URIs for creditor's remaining credit-issuing transfers.",
        example=['123e4567-e89b-12d3-a456-426655440000', '183ea7c7-7a96-4ed7-a50a-a2b069687d23'],
    )

    def get_uri(self, obj):
        return url_for(self.context['TransfersCollection'], _external=True, creditorId=obj.creditor_id)


class PortfolioSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/portfolio',
    )
    type = fields.Constant(
        'Portfolio',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Portfolio',
    )
    accountRecords = fields.Nested(
        PaginatedListSchema(many=False),
        required=True,
        description='A paginated list of URIs for all account records that belong to the creditor.',
    )


class AccountCreationRequestSchema(Schema):
    debtor_uri = fields.Url(
        required=True,
        relative=True,
        schemes=[endpoints.get_url_scheme()],
        data_key='debtorUri',
        format='uri',
        description="The debtor's URI.",
        example='https://example.com/debtors/1',
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
    type = fields.Constant(
        'AccountRecordConfig',
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
    config_changed_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='configChangedAt',
        description='The moment at which the last change in the account configuration was made.',
    )
    config_is_effectual = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='configIsEffectual',
        description='Whether the current configuration is effectual.',
        example=True,
    )
    is_scheduled_for_deletion = fields.Boolean(
        missing=False,
        data_key='isScheduledForDeletion',
        description='Whether the account is scheduled for deletion. Most of the time, to safely '
                    'delete an acount, it should be first scheduled for deletion, and deleted '
                    'only after the corresponding account record has been marked as safe for '
                    'deletion. Not passing this field has the same effect as passing `false`.',
        example=False,
    )
    negligible_amount = fields.Float(
        missing=0.0,
        validate=validate.Range(min=0.0),
        data_key='negligibleAmount',
        description='The maximum amount that is considered negligible. It is used to '
                    'decide whether the account can be safely deleted, and whether a '
                    'transfer should be considered as insignificant. Must be '
                    'non-negative. Not passing this field has the same effect as '
                    'passing `0`.',
        example=0.0,
    )


class LedgerEntrySchema(Schema):
    entryId = fields.Integer(
        required=True,
        dump_only=True,
        format="int64",
        description="The ID of this entry. Later entries have bigger IDs.",
        example='123',
    )
    accountRecordUri = fields.Method(
        'get_account_record_uri',
        required=True,
        type='string',
        format="uri",
        description="The URI of the corresponding account record.",
        example='https://example.com/creditors/2/accounts/1/',
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format="int64",
        description="The amount added to account's principal. Can be positive (an increase) or "
                    "negative (a decrease). Can not be zero.",
        example=1000,
    )
    balance = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format="int64",
        description='The new principal amount on the account, as it is after the transfer. Unless '
                    'a principal overflow has occurred, the new principal amount will be equal to '
                    '`amount` plus the old principal amount.',
        example=1500,
    )
    transferUri = fields.Method(
        'get_transfer_uri',
        required=True,
        type='string',
        format="uri",
        description='The URI of the corresponding transfer.',
        example='https://example.com/creditors/2/accounts/1/transfers/',
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
        format="int64",
        description="The ID of the previous entry in the account's ledger. Previous entries have "
                    "smaller IDs. When this field is not present, this means that there are no "
                    "previous entries.",
        example='122',
    )


class LedgerEntriesPage(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/entries?first=123',
    )
    type = fields.Constant(
        'LedgerEntriesPage',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntriesPage',
    )
    items = fields.Nested(
        LedgerEntrySchema(many=True),
        required=True,
        dump_only=True,
        description='An array of ledger entries. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description='An URI of another `LedgerEntriesPage` object which contains more items. When '
                    'there are no remaining items, this field will not be present. If this field '
                    'is present, there might be remaining items, even when the `items` array is '
                    'empty. This can be a relative URI.',
        example='?first=122',
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
    type = fields.Constant(
        'Account',
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
        example='https://example.com/debtors/1',
    )
    creditorUri = fields.Function(
        lambda obj: endpoints.build_url('creditor', creditorId=obj.creditor_id),
        required=True,
        type='string',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/creditors/2',
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
    type = fields.Constant(
        'AccountRecord',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountRecord',
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
        PaginatedListSchema(many=False),
        required=True,
        description='A paginated list of account ledger entries. That is: transfers for '
                    'which the account is either the sender or the recipient. The paginated '
                    'list will be sorted in reverse-chronological order (bigger entry IDs go '
                    'first). The entries will constitute a singly linked list, each entry '
                    '(except the most ancient one) referring to its ancestor.',
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


class CommittedTransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/transfers/54321',
    )
    type = fields.Constant(
        'CommittedTransfer',
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='CommittedTransfer',
    )
    senderAccountUri = fields.Function(
        lambda obj: endpoints.build_url('account', creditorId=obj.sender_creditor_id, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The sender's account URI.",
        example='https://example.com/creditors/2/debtors/1',
    )
    recipientAccountUri = fields.Function(
        lambda obj: endpoints.build_url('account', creditorId=obj.sender_creditor_id, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The recipient's account URI.",
        example='https://example.com/creditors/3/debtors/1',
    )
    committed_amount = fields.Integer(
        required=True,
        dump_only=True,
        data_key='committedAmount',
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description='The transferred amount.',
        example=1000,
    )
    committed_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='committedAt',
        description='The moment at which the transfer was committed.',
    )
    coordinator_type = fields.String(
        required=True,
        dump_only=True,
        data_key='transferClass',
        description='The transfer class.',
        example='direct',
    )
    transfer_info = fields.Dict(
        required=True,
        dump_only=True,
        data_key='transferInfo',
        description=InitiatedTransfer.transfer_info.comment,
    )
