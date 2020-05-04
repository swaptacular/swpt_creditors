from marshmallow import Schema, fields, validate
from flask import url_for
from .common import (
    ObjectReferenceSchema, AccountInfoSchema, TransferStatusSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION,
)

_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_NOTES_DESCRIPTION = '\
Notes from the sender. Can be any JSON object containing information that \
the sender wants the recipient to see. Different debtors may impose \
different restrictions on the schema and the contents of of this object.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_DEBTOR_URI_DESCRIPTION = '\
The URI of the debtor through which the transfer should go. This is analogous to \
the currency code in "normal" bank transfers.'


class BaseTransferSchema(Schema):
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
        format='int64',
        description='The transferred amount.',
        example=1000,
    )


class TransferCreationRequestSchema(Schema):
    type = fields.String(
        missing='TransferCreationRequest',
        description='The type of this object.',
        example='TransferCreationRequest',
    )
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
        format='int64',
        description='The amount to be transferred. Must be positive.',
        example=1000,
    )
    notes = fields.Dict(
        missing={},
        description=_TRANSFER_NOTES_DESCRIPTION,
    )


class TransferSchema(BaseTransferSchema):
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
        example='Transfer',
    )
    portfolio = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Portfolio` that contains this transfer.",
        example={'uri': '/creditors/2/portfolio'},
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
    latestUpdateEntryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description='The ID of the latest `TransferUpdate` entry for this transfer in '
                    'the log. It gets bigger after each update.',
        example=345,
    )
    status = fields.Nested(
        TransferStatusSchema,
        required=True,
        dump_only=True,
        description="The transfer's `TransferStatus`.",
    )

    def get_uri(self, obj):
        return url_for(
            self.context['Transfer'],
            _external=True,
            creditorId=obj.creditor_id,
            transferUuid=obj.transfer_uuid,
        )


class CancelTransferRequestSchema(Schema):
    type = fields.String(
        missing='CancelTransferRequest',
        description='The type of this object.',
        example='CancelTransferRequest',
    )


class CommittedTransferSchema(BaseTransferSchema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/transfers/999',
    )
    type = fields.Function(
        lambda obj: 'CommittedTransfer',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different debtors may use different '
                    '**additional fields**, providing more information about the transfer '
                    '(notes from the sender for example). This field contains the name '
                    'of the used schema.',
        example='CommittedTransfer',
    )
    committed_at_ts = fields.DateTime(
        dump_only=True,
        data_key='committedAt',
        description='The moment at which the transfer was committed. If this field is '
                    'not present, this means that the moment at which the transfer was '
                    'committed is unknown.',
    )
