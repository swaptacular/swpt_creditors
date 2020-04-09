from datetime import datetime, timezone, timedelta
from typing import NamedTuple, List
from marshmallow import Schema, fields, validate
from flask import url_for, current_app
from swpt_lib import endpoints
from .common import MAX_INT64


_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_INFO_DESCRIPTION = '\
Notes from the sender. Can be any JSON object that the sender wants the recipient to see.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_IS_SUCCESSFUL_DESCRIPTION = '\
Whether the transfer has been successful or not.'


class TransfersCollection(NamedTuple):
    creditor_id: int
    items: List[str]


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
        example='https://example.com/debtors/1/',
    )
    recipient_account_uri = fields.Url(
        required=True,
        relative=False,
        require_tld=True,
        schemes=[endpoints.get_url_scheme()],
        data_key='recipientAccountUri',
        format='uri',
        description="The recipient's account URI.",
        example='https://example.com/creditors/2222/debtors/1',
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description='The amount to be transferred. Must be positive.',
        example=1000,
    )
    transfer_info = fields.Dict(
        missing={},
        data_key='transferInfo',
        description='Notes from the sender. Can be any JSON object that the sender wants the '
                    'recipient to see.',
    )


class TransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000',
    )
    type = fields.Function(
        lambda obj: 'Transfer',
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
        example='https://example.com/debtors/1/',
    )
    senderAccountUri = fields.Function(
        lambda obj: endpoints.build_url('account', creditorId=obj.creditor_id, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The sender's account URI.",
        example='https://example.com/creditors/2/debtors/1',
    )
    recipientAccountUri = fields.Function(
        lambda obj: endpoints.build_url('account', creditorId=obj.creditor_id, debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The recipient's account URI.",
        example='https://example.com/creditors/2222/debtors/1',
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=_TRANSFER_AMOUNT_DESCRIPTION,
        example=1000,
    )
    transfer_info = fields.Dict(
        required=True,
        dump_only=True,
        data_key='transferInfo',
        description=_TRANSFER_INFO_DESCRIPTION,
    )
    initiated_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='initiatedAt',
        description=_TRANSFER_INITIATED_AT_TS_DESCRIPTION,
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
        description=_TRANSFER_IS_SUCCESSFUL_DESCRIPTION,
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


class TransferUpdateRequestSchema(Schema):
    is_finalized = fields.Boolean(
        required=True,
        data_key='isFinalized',
        description='Should be `true`.',
        example=True,
    )
    is_successful = fields.Boolean(
        required=True,
        data_key='isSuccessful',
        description='Should be `false`.',
        example=False,
    )


class TransfersCollectionSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/debtors/1/transfers/',
    )
    type = fields.Function(
        lambda obj: 'TransfersCollection',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='TransfersCollection',
    )
    debtorUri = fields.Function(
        lambda obj: endpoints.build_url('debtor', debtorId=obj.debtor_id),
        required=True,
        type='string',
        format="uri",
        description="The debtor's URI.",
        example='https://example.com/debtors/1/',
    )
    totalItems = fields.Function(
        lambda obj: len(obj.items),
        required=True,
        type='integer',
        description="The total number of items in the collection.",
        example=2,
    )
    items = fields.List(
        fields.Str(format='uri-reference'),
        dump_only=True,
        description="When the total number of items in the collection is small enough, this field "
                    "will contain all of them (in an array), so that in such cases it would be "
                    "unnecessary to follow the `first` link.",
        example=['123e4567-e89b-12d3-a456-426655440000', '183ea7c7-7a96-4ed7-a50a-a2b069687d23'],
    )
    itemsType = fields.Function(
        lambda obj: 'string',
        required=True,
        type='string',
        description='The type of the items in the collection. In this particular case the items '
                    'are relative URIs, so the type will be `"string"`.',
        example='string',
    )
    first = fields.Function(
        lambda obj: '',
        required=True,
        type='string',
        format="uri-reference",
        description='The URI of the first page in the paginated collection. The object retrieved '
                    'from this URI will have: 1) An `items` property (an array), which will contain '
                    'the first items of the collection; 2) May have a `next` property (a string), '
                    'which would contain the URI of the next page in the collection. This can be '
                    'a relative URI.',
        example='',
    )

    def get_uri(self, obj):
        return url_for(self.context['TransfersCollection'], _external=True, debtorId=obj.debtor_id)


class CommittedTransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/accounts/1/transfers/999',
    )
    type = fields.Function(
        lambda obj: 'CommittedTransfer',
        required=True,
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
    commitMessage = fields.Dict(
        dump_only=True,
        description='An optional JSON object containing additional information about the '
                    'transfer, notably -- notes from the sender. Different implementations '
                    'may use different schemas for this object. A `type` field must always '
                    'be present, containing the name of the used schema.',
    )
