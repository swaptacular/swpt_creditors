from marshmallow import Schema, fields, validate, missing
from flask import url_for
from .common import (
    ObjectReferenceSchema, AccountIdentitySchema, MutableResourceSchema,
    MAX_INT32, MIN_INT64, MAX_INT64, URI_DESCRIPTION,
)

_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_DEBTOR_URI_DESCRIPTION = '\
The URI of the debtor through which the transfer should go. This is analogous to \
the currency code in "normal" bank transfers.'


class TransferErrorSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferError',
        required=True,
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
    totalLockedAmount = fields.Integer(
        required=True,
        dump_only=True,
        format="int64",
        description='The total amount secured (locked) for prepared transfers on the account.',
        example=0,
    )


class TransferOptionsSchema(Schema):
    type = fields.String(
        missing='TransferOptions',
        description='The type of this object.',
        example='TransferOptions',
    )
    min_amount = fields.Integer(
        missing=0,
        validate=validate.Range(min=0, max=MAX_INT64),
        format='int64',
        data_key='minAmount',
        description='The secured amount should be equal or bigger than this value.',
        example=0,
    )
    max_amount = fields.Integer(
        missing=0,
        validate=validate.Range(min=0, max=MAX_INT64),
        format='int64',
        data_key='maxAmount',
        description='The secured amount should not exceed this value. Note that this value '
                    'must be equal or bigger than the value of `min_amount`.',
        example=0,
    )
    min_interest_rate = fields.Float(
        missing=-100.0,
        validate=validate.Range(min=-100.0),
        data_key='minInterestRate',
        description='The minimal acceptable interest rate. If the interest rate on the '
                    'account becomes lower than this value, the transfer will not be '
                    'successful. This can be useful when the transferred amount may need '
                    'to be decreased if the interest rate on the account has decreased.',
        example=-100.0,
    )
    max_commit_delay = fields.Integer(
        missing=MAX_INT32,
        validate=validate.Range(min=0, max=MAX_INT32),
        format='int32',
        data_key='maxCommitDelay',
        description='The period (in seconds) during which the prepared transfer can be '
                    'committed successfully.',
        example=MAX_INT32,
    )


class TransferResultSchema(Schema):
    type = fields.Function(
        lambda obj: 'TransferResult',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransferResult',
    )
    finalized_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='finalizedAt',
        description='The moment at which the transfer was finalized.',
    )
    committedAmount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=0, max=MAX_INT64),
        format='int64',
        description='The transferred amount. If the transfer has been successful, the value will '
                    'be equal to the requested transfer amount. If the transfer has been '
                    'unsuccessful, the value will be zero.',
        example=0,
    )
    error = fields.Nested(
        TransferErrorSchema,
        dump_only=True,
        description='An error that has occurred during the execution of the transfer. This field '
                    'will be present if, and only if, the transfer has been unsuccessful.',
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
    recipient = fields.Nested(
        AccountIdentitySchema,
        required=True,
        description="The recipient's `AccountIdentity` information.",
        example={'uri': 'swpt:1/2222'}
    )
    amount = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        format='int64',
        description='The amount that has to be transferred. Must be a positive number.',
        example=1000,
    )
    options = fields.Nested(
        TransferOptionsSchema,
        missing={},
        description="Transfer's `TransferOptions`.",
    )
    note = fields.Dict(
        missing={},
        description='A note from the sender. Can be any JSON object containing information '
                    'that the sender wants the recipient to see.',
    )


class TransferSchema(TransferCreationRequestSchema, MutableResourceSchema):
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
    transferList = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of creditor's `TransferList`.",
        example={'uri': '/creditors/2/transfer-list'},
    )
    initiated_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='initiatedAt',
        description=_TRANSFER_INITIATED_AT_TS_DESCRIPTION,
    )
    checkup_at_ts = fields.Method(
        'get_checkup_at_ts',
        type='string',
        format='date-time',
        data_key='checkupAt',
        description="The moment at which the sender is advised to look at the transfer "
                    "again, to see if it's status has changed. If this field is not present, "
                    "this means either that the status of the transfer is not expected to "
                    "change, or that the moment of the expected change can not be predicted. "
                    "Note that the value of this field is calculated on-the-fly, so it may "
                    "change from one request to another, and no `LogEntry` for the change "
                    "will be added to the log.",
    )
    result = fields.Nested(
        TransferResultSchema,
        dump_only=True,
        description='Contains information about the outcome of the transfer. This field will '
                    'be preset if, and only if, the transfer has been finalized. Note that a '
                    'finalized transfer can be either successful, or unsuccessful.',
    )

    def get_uri(self, obj):
        return url_for(
            self.context['Transfer'],
            _external=True,
            creditorId=obj.creditor_id,
            transferUuid=obj.transfer_uuid,
        )

    def get_checkup_at_ts(self, obj):
        return missing


class CancelTransferRequestSchema(Schema):
    type = fields.String(
        missing='CancelTransferRequest',
        description='The type of this object.',
        example='CancelTransferRequest',
    )


class CommittedTransferSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/transfers/18444/999',
    )
    type = fields.Function(
        lambda obj: 'CommittedTransfer',
        required=True,
        type='string',
        description='The type of this object.',
        example='CommittedTransfer',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the affected `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    coordinator = fields.String(
        required=True,
        dump_only=True,
        description='Indicates the subsystem which requested the transfer.',
        example='direct',
    )
    sender = fields.Nested(
        AccountIdentitySchema,
        dump_only=True,
        description="The sender's `AccountIdentity` information. When this field is not "
                    "present, this means that the sender is unknown.",
        example={'uri': 'swpt:1/2'}
    )
    recipient = fields.Nested(
        AccountIdentitySchema,
        dump_only=True,
        description="The recipient's `AccountIdentity` information. When this field is not "
                    "present, this means that the recipient is unknown.",
        example={'uri': 'swpt:1/2222'}
    )
    acquiredAmount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
        format='int64',
        description="The amount that this transfer has added to the account's principal. This "
                    "can be a positive number (an incoming transfer), a negative number (an "
                    "outgoing transfer), or zero (a dummy transfer).",
        example=1000,
    )
    note = fields.Dict(
        missing={},
        dump_only=True,
        description='A note from the committer of the transfer. Can be any JSON object '
                    'containing information that whoever committed the transfer wants the '
                    'recipient (and the sender) to see.',
    )
    committed_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='committedAt',
        description='The moment at which the transfer was committed.',
    )
