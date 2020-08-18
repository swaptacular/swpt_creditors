import json
from copy import copy
from marshmallow import Schema, fields, validate, missing, pre_dump
from swpt_lib.utils import i64_to_u64
from swpt_lib.swpt_uris import make_account_uri
from swpt_creditors import models
from swpt_creditors.models import MAX_INT64
from .common import ObjectReferenceSchema, AccountIdentitySchema, MutableResourceSchema, URI_DESCRIPTION

_TRANSFER_AMOUNT_DESCRIPTION = '\
The amount to be transferred. Must be positive.'

_TRANSFER_INITIATED_AT_TS_DESCRIPTION = '\
The moment at which the transfer was initiated.'

_TRANSFER_DEBTOR_URI_DESCRIPTION = '\
The URI of the debtor through which the transfer should go. This is analogous to \
the currency code in "normal" bank transfers.'


def _make_invalid_account_uri(debtor_id: int) -> str:
    return f'swpt:{i64_to_u64(debtor_id)}/!'


def _parse_transfer_note(transfer_note):
    # TODO: Move this function to `swpt_lib.utils`.

    if transfer_note == '':
        return {}

    try:
        note = json.loads(transfer_note)
    except json.JSONDecodeError:
        note = None

    if not isinstance(note, dict):
        note = {'type': 'TextMessage', 'content': transfer_note}

    return note


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
        missing=0,
        dump_only=True,
        format="int64",
        description='The total amount secured (locked) for transfers on the account.',
        example=0,
    )


class TransferOptionsSchema(Schema):
    type = fields.String(
        missing='TransferOptions',
        default='TransferOptions',
        description='The type of this object.',
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
    deadline = fields.DateTime(
        description='The transfer will be successful only if it is committed before this moment. '
                    'This can be useful, for example, when the transferred amount may need to be '
                    'decreased if the transfer can not be committed in time. When this field is '
                    'not present, this means that the deadline for the transfer will not be '
                    'earlier than normal.',
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
        format='int64',
        description='The transferred amount. If the transfer has been successful, the value will '
                    'be equal to the requested transfer amount (always a positive number). If '
                    'the transfer has been unsuccessful, the value will be zero.',
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
        default='TransferCreationRequest',
        description='The type of this object.',
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
        example={'type': 'AccountIdentity', 'uri': 'swpt:1/2222'}
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
        required=True,
        description='A note from the sender. Can be any JSON object that contains information '
                    'which the sender wants the recipient to see.',
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
                    "change, or that the moment of the expected change can not be predicted."
                    "\n\n"
                    "**Note:** The value of this field is calculated on-the-fly, so it may "
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
        return self.context['path'].get_transfer(creditorId=obj.creditor_id, transferUuid=obj.transfer_uuid)

    def get_checkup_at_ts(self, obj):
        return missing


class CancelTransferRequestSchema(Schema):
    type = fields.String(
        missing='CancelTransferRequest',
        default='CancelTransferRequest',
        description='The type of this object.',
    )


class CommittedTransferSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/transfers/18444-999',
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
    coordinator_type = fields.String(
        missing='direct',
        dump_only=True,
        data_key='coordinator',
        description='Indicates the subsystem which requested the transfer.',
        example='direct',
    )
    sender = fields.Nested(
        AccountIdentitySchema,
        required=True,
        dump_only=True,
        description="The sender's `AccountIdentity` information.",
        example={'type': 'AccountIdentity', 'uri': 'swpt:1/2'}
    )
    recipient = fields.Nested(
        AccountIdentitySchema,
        required=True,
        dump_only=True,
        description="The recipient's `AccountIdentity` information.",
        example={'type': 'AccountIdentity', 'uri': 'swpt:1/2222'}
    )
    acquired_amount = fields.Integer(
        required=True,
        dump_only=True,
        format='int64',
        data_key='acquiredAmount',
        description="The amount that this transfer has added to the account's principal. This "
                    "can be a positive number (an incoming transfer), a negative number (an "
                    "outgoing transfer), or zero (a dummy transfer).",
        example=1000,
    )
    note = fields.Dict(
        required=True,
        dump_only=True,
        description='A note from the committer of the transfer. Can be any JSON object that '
                    'contains information which whoever committed the transfer wants the '
                    'recipient (and the sender) to see.',
    )
    committed_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='committedAt',
        description='The moment at which the transfer was committed.',
    )

    @pre_dump
    def process_committed_transfer_instance(self, obj, many):
        assert isinstance(obj, models.CommittedTransfer)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.committed_transfer(
            creditorId=obj.creditor_id,
            debtorId=obj.debtor_id,
            creationDate=obj.creation_date,
            transferNumber=obj.transfer_number,
        )
        obj.account = {'uri': paths.account(creditorId=obj.creditor_id, debtorId=obj.debtor_id)}

        try:
            sender_uri = make_account_uri(obj.debtor_id, obj.sender_id)
        except ValueError:
            sender_uri = _make_invalid_account_uri(obj.debtor_id)
        obj.sender = {'uri': sender_uri}

        try:
            recipient_uri = make_account_uri(obj.debtor_id, obj.recipient_id)
        except ValueError:
            recipient_uri = _make_invalid_account_uri(obj.debtor_id)
        obj.recipient = {'uri': recipient_uri}

        obj.note = _parse_transfer_note(obj.transfer_note)

        return obj
