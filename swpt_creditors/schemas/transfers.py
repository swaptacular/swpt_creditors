from marshmallow import Schema, fields, validate
from flask import url_for
from .common import (
    ObjectReferenceSchema, AccountIdentitySchema, TransferStatusSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION, LATEST_UPDATE_AT_DESCRIPTION,
)

_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_DEBTOR_URI_DESCRIPTION = '\
The URI of the debtor through which the transfer should go. This is analogous to \
the currency code in "normal" bank transfers.'


class BaseTransferSchema(Schema):
    sender = fields.Nested(
        AccountIdentitySchema,
        required=True,
        dump_only=True,
        description="The sender's `AccountIdentity` information.",
        example={'type': 'SwptAccount', 'debtorId': 1, 'creditorId': 2222},
    )
    recipient = fields.Nested(
        AccountIdentitySchema,
        required=True,
        description="The recipient's `AccountIdentity` information.",
        example={'type': 'SwptAccount', 'debtorId': 1, 'creditorId': 2222},
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format='int64',
        description='The transferred amount. Must be positive.',
        example=1000,
    )
    payee_ref = fields.String(
        missing='',
        data_key='payeeRef',
        description='The *payee reference*. A payee reference is a short string that may be '
                    'included with transfers to help the recipient to identify the sender '
                    'and/or the reason for the transfer.',
        example='PAYMENT 123',
    )
    payer_ref = fields.String(
        missing='',
        data_key='payerRef',
        description='The *payer reference*. A payer reference is a short string that may be '
                    'be included with transfers to help the sender to identify the transfer. '
                    'For example, this can be useful when the recipient is making a refund, '
                    'to refer to the original payment.',
        example='PAYMENT ABC',
    )
    notes = fields.Dict(
        missing={},
        description='Notes from the sender. Can be any JSON object containing information that '
                    'the sender wants the recipient to see. Different debtors may impose '
                    'different restrictions on the schema and the contents of of this object.',
    )


class TransferCreationRequestSchema(BaseTransferSchema):
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
        type='string',
        description='The type of this object.',
        example='Transfer',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet` that contains this transfer.",
        example={'uri': '/creditors/2/wallet'},
    )
    initiated_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='initiatedAt',
        description=_TRANSFER_INITIATED_AT_TS_DESCRIPTION,
    )
    latestUpdateId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_UINT64),
        format='uint64',
        description='The ID of the latest `TransferUpdate` entry for this transfer in '
                    'the log. It gets bigger after each update.',
        example=345,
    )
    latestUpdateAt = fields.DateTime(
        required=True,
        dump_only=True,
        description=LATEST_UPDATE_AT_DESCRIPTION.format(type='TransferUpdate'),
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
