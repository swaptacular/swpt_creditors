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
        required=True,
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
        required=True,
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
        required=True,
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
        fields.Str(format='uri-reference'),
        required=True,
        dump_only=True,
        description="An unordered set of *relative* URIs for creditor's remaining credit-issuing transfers.",
        example=['123e4567-e89b-12d3-a456-426655440000', '183ea7c7-7a96-4ed7-a50a-a2b069687d23'],
    )

    def get_uri(self, obj):
        return url_for(self.context['TransfersCollection'], _external=True, creditorId=obj.creditor_id)


class AccountsCollectionSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/1/accounts/',
    )
    type = fields.Constant(
        'AccountsCollection',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountsCollection',
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
        fields.Str(format='uri-reference'),
        required=True,
        dump_only=True,
        description="An unordered set of *relative* URIs for creditor accounts.",
        example=['1234', '5678'],
    )

    def get_uri(self, obj):
        return url_for(self.context['AccountsCollection'], _external=True, debtorId=obj.debtor_id)


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


class AccountConfigSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/config',
    )
    type = fields.Constant(
        'AccountConfig',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountConfig',
    )
    accountRecordUri = fields.Method(
        'get_account_record_uri',
        required=True,
        type='string',
        format="uri",
        description="The URI of the corresponding account record.",
        example='https://example.com/creditors/2/accounts/1',
    )
    last_change_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='lastChangeAt',
        description='The last time at which the configuration was changed.',
    )
    last_change_is_effectual = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='lastChangeIsEffectual',
        description='Whether the configuration is currently effectual.',
        example=True,
    )
    is_scheduled_for_deletion = fields.Boolean(
        required=True,
        data_key='isScheduledForDeletion',
        description='Whether the account is scheduled for deletion. To safely delete an '
                    'acount, it should be first scheduled for deletion, and deleted only '
                    'after the corresponding account record has been marked as safe for '
                    'deletion.',
        example=False,
    )
    allow_unsafe_deletion = fields.Boolean(
        required=True,
        data_key='allowUnsafeDeletion',
        description='Whether the account record can be forcefully deleted, potentially '
                    'losing a non-negligible amount of money on the account. Normally, it '
                    'should be `false`.',
        example=False,
    )
    negligible_amount = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        data_key='negligibleAmount',
        description='The maximum amount that is considered negligible. It is used to '
                    'decide whether the account can be safely deleted, and whether a '
                    'transfer should be considered as insignificant. Must be non-negative.',
        example=0.0,
    )


class AccountConfigChangeRequestSchema(AccountConfigSchema):
    class Meta:
        exclude = ['uri', 'type', 'accountRecordUri', 'last_change_at', 'last_change_is_effectual']


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
        example='https://example.com/creditors/2/accounts/1',
    )
    type = fields.Constant(
        'AccountRecord',
        required=True,
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
        description="The account URI.",
        example='https://example.com/creditors/2/debtors/1',
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
    interestRate = fields.Method(
        'get_interest_rate',
        required=True,
        dump_only=True,
        format="float",
        description='Annual rate (in percents) at which interest accumulates on the account.',
        example=0.0,
    )
    transfersUri = fields.Method(
        'get_transfers_uri',
        required=True,
        type='string',
        format="uri",
        description="The URI for the list of recent account transfers.",
        example='https://example.com/creditors/2/accounts/1/transfers/',
    )
    config = fields.Nested(
        AccountConfigSchema(exclude=['type', 'accountRecordUri']),
        dump_only=True,
        required=True,
        description="The account's configuration. Can be changed by the owner of the account.",
    )
    is_deletion_safe = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isDeletionSafe',
        description='Whether it is safe to delete this account record.',
        example=False,
    )

    def get_uri(self, obj):
        return url_for(
            self.context['AccountRecord'],
            _external=True,
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
        )
