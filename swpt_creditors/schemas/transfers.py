from datetime import datetime, timezone, timedelta
from marshmallow import Schema, fields, validate
from flask import url_for, current_app
from swpt_lib import endpoints
from .common import ObjectReferenceSchema, MAX_INT64, URI_DESCRIPTION

_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_INFO_DESCRIPTION = '\
Notes from the sender. Can be any JSON object that the sender wants the \
recipient to see. Different implementations may impose different restrictions \
on the schema of this JSON object.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_IS_SUCCESSFUL_DESCRIPTION = '\
Whether the transfer has been successful or not.'

_TRANSFER_DEBTOR_URI_DESCRIPTION = '\
The URI of the debtor through which the transfer should go. This is analogous to \
the currency code in "normal" bank transfers.'


class DirectTransferErrorSchema(Schema):
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
        description=_TRANSFER_DEBTOR_URI_DESCRIPTION,
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
    info = fields.Dict(
        missing={},
        description=_TRANSFER_INFO_DESCRIPTION,
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
    debtor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description=_TRANSFER_DEBTOR_URI_DESCRIPTION,
        example={'uri': 'https://example.com/debtors/1/'},
    )
    senderAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The sender's account URI.",
        example={'uri': '/creditors/2/debtors/1'},
    )
    recipientAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The recipient's account URI.",
        example={'uri': 'https://example.com/creditors/2222/debtors/1'},
    )
    amount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format="int64",
        description=_TRANSFER_AMOUNT_DESCRIPTION,
        example=1000,
    )
    info = fields.Dict(
        required=True,
        dump_only=True,
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
        DirectTransferErrorSchema(many=True),
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
    senderAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The sender's account URI.",
        example={'uri': 'https://example.com/creditors/2/debtors/1'},
    )
    recipientAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The recipient's account URI.",
        example={'uri': 'https://example.com/creditors/3/debtors/1'},
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
                    'transfer, notably -- notes from the sender. Different implementations '
                    'may use different schemas for this object. A `type` field must always '
                    'be present, containing the name of the used schema.',
    )
