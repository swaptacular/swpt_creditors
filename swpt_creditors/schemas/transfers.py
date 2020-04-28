from datetime import datetime, timezone, timedelta
from marshmallow import Schema, fields, validate
from flask import url_for, current_app
from .common import AccountInfoSchema, MAX_INT64, URI_DESCRIPTION

_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_NOTES_DESCRIPTION = '\
Notes from the sender. Can be any JSON object that the sender wants the \
recipient to see. Different debtor types may impose different restrictions \
on the schema and the contents of of this object.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_IS_SUCCESSFUL_DESCRIPTION = '\
Whether the transfer has been successful or not.'

_TRANSFER_DEBTOR_URI_DESCRIPTION = '\
The URI of the debtor through which the transfer should go. This is analogous to \
the currency code in "normal" bank transfers.'


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferError',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='TransferError',
    )
    errorCode = fields.String(
        required=True,
        dump_only=True,
        description='The error code.',
        example='INSUFFICIENT_AVAILABLE_AMOUNT',
    )
    avlAmount = fields.Integer(
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
    recipient_account_info = fields.Nested(
        AccountInfoSchema,
        required=True,
        data_key='recipientAccountInfo',
        description="The recipient's account information.",
        example={'type': 'SwptAccountInfo', 'debtorId': 1, 'creditorId': 2222},
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description='The amount to be transferred. Must be positive.',
        example=1000,
    )
    notes = fields.Dict(
        missing={},
        description=_TRANSFER_NOTES_DESCRIPTION,
    )


class DirectTransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000',
    )
    type = fields.Function(
        lambda obj: 'Transfer',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='DirectTransfer',
    )
    senderAccountInfo = fields.Nested(
        AccountInfoSchema,
        required=True,
        dump_only=True,
        description="The sender's account information.",
        example={'type': 'SwptAccountInfo', 'debtorId': 1, 'creditorId': 2},
    )
    recipientAccountInfo = fields.Nested(
        AccountInfoSchema,
        required=True,
        dump_only=True,
        description="The recipient's account information.",
        example={'type': 'SwptAccountInfo', 'debtorId': 1, 'creditorId': 2222},
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=_TRANSFER_AMOUNT_DESCRIPTION,
        example=1000,
    )
    notes = fields.Dict(
        required=True,
        dump_only=True,
        description=_TRANSFER_NOTES_DESCRIPTION,
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
        description='Errors that have occurred during the execution of the transfer. If '
                    'the transfer has been successful, this will be an empty array.',
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


class DirectTransferUpdateRequestSchema(Schema):
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


class TransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/transfers/999',
    )
    type = fields.Function(
        lambda obj: 'Transfer',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Transfer',
    )
    senderAccountInfo = fields.Nested(
        AccountInfoSchema,
        required=True,
        dump_only=True,
        description="The sender's account information.",
        example={'type': 'SwptAccountInfo', 'debtorId': 1, 'creditorId': 2222},
    )
    recipientAccountInfo = fields.Nested(
        AccountInfoSchema,
        required=True,
        dump_only=True,
        description="The recipient's account information.",
        example={'type': 'SwptAccountInfo', 'debtorId': 1, 'creditorId': 2222},
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description='The transferred amount.',
        example=1000,
    )
    committed_at_ts = fields.DateTime(
        dump_only=True,
        data_key='committedAt',
        description='The moment at which the transfer was committed. If this field is '
                    'not present, this means that the moment at which the transfer was '
                    'committed is unknown.',
    )
    details = fields.Dict(
        dump_only=True,
        description='An optional JSON object containing additional information about the '
                    'transfer, notably -- notes from the sender. Different debtor types '
                    'may use different schemas for this object. A `type` field must always '
                    'be present, containing the name of the used schema.',
    )
