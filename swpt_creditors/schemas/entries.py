from marshmallow import Schema, fields, validate
from .common import (
    ObjectReferenceSchema, TransferErrorSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION, PAGE_FORTHCOMING_DESCRIPTION,
)


class LogEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LogEntry',
        required=True,
        type='string',
        description='The type of this object. Different kinds of log entries may use different '
                    '**additional fields**, providing more data. This field contains the name '
                    'of the used schema.',
        example='LogEntry',
    )
    entry_id = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format='uint64',
        data_key='entryId',
        description='The ID of this log entry. Later log entries have bigger IDs.',
        example=12345,
    )
    posted_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='postedAt',
        description='The moment at which this entry was added to the log.',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the object that has been created, updated, or deleted.",
        example={'uri': '/objects/123'},
    )
    deleted = fields.Boolean(
        dump_only=True,
        missing=False,
        description="Whether the object has been deleted.",
    )


class LogEntriesPageSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/log',
    )
    type = fields.Function(
        lambda obj: 'LogEntriesPage',
        required=True,
        type='string',
        description='The type of this object.',
        example='LogEntriesPage',
    )
    items = fields.Nested(
        LogEntrySchema(many=True),
        required=True,
        dump_only=True,
        description='An array of `LogEntry`s. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='LogEntriesPage'),
        example='?prev=12345',
    )
    forthcoming = fields.Method(
        'get_forthcoming_uri',
        type='string',
        format='uri-reference',
        description=PAGE_FORTHCOMING_DESCRIPTION.format(type='LogEntriesPage'),
        example='?prev=1234567890',
    )


class LedgerUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'LedgerUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='LedgerUpdate',
    )
    aquiredAmount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description="The amount added to the account's principal. Can be a positive number (an "
                    "increase), or a negative number (a decrease). Can not be zero.",
        example=1000,
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description='The new principal amount on the account, as it is after the transfer. Unless '
                    'a principal overflow has occurred, the new principal amount will be equal to '
                    '`aquiredAmount` plus the old principal amount.',
        example=1500,
    )
    transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the corresponding `CommittedTransfer`.',
        example={'uri': '/creditors/2/accounts/1/transfers/18444/999'},
    )
    previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        validate=validate.Range(min=1, max=MAX_UINT64),
        format='uint64',
        description="The `entryId` of the previous `LedgerUpdate` log entry for this account. "
                    "Previous entries have smaller IDs. When this field is not present, this "
                    "means that there are no previous entries in the account's ledger.",
        example=122,
    )


class LedgerUpdatesPageSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/entries?prev=124',
    )
    type = fields.Function(
        lambda obj: 'LedgerUpdatesPage',
        required=True,
        type='string',
        description='The type of this object.',
        example='LedgerUpdatesPage',
    )
    items = fields.Nested(
        LedgerUpdateSchema(many=True),
        required=True,
        dump_only=True,
        description='An array of `LedgerUpdate`s. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='LedgerUpdatesPage'),
    )


class AccountCommitSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountCommit',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountCommit',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the affected `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    acquiredAmount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description="The amount that will eventually be added to the affected account's "
                    "principal. Can be a positive number (the affected account is the "
                    "recipient), a negative number (the affected account is the sender), "
                    "or zero (a dummy transfer).",
        example=1000,
    )
    reference = fields.String(
        dump_only=True,
        description="An optional payment reference. A payment reference is a short string "
                    "that may be included with transfers to help identify the transfer. For "
                    "incoming transfers this will be the transfer's *payee reference*. For "
                    "outgoing transfers this will be the transfer's *payer reference*.",
        example='PAYMENT 123',
    )


class AccountUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountUpdate',
    )


class AccountInfoUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountInfoUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountInfoUpdate',
    )


class AccountKnowledgeUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountKnowledgeUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountKnowledgeUpdate',
    )


class AccountConfigUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountConfigUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountConfigUpdate',
    )


class AccountExchangeUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountExchangeUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountExchangeUpdate',
    )


class AccountDisplayUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountDisplayUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountDisplayUpdate',
    )


class TransferUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'TransferUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransferUpdate',
    )
    finalized_at_ts = fields.DateTime(
        dump_only=True,
        data_key='finalizedAt',
        description='The value of the `finalizedAt` field in the created/updated `Transfer`. '
                    'This field will not be present when the transfer has been deleted, or '
                    'when the field is not present in the created/updated transfer.',
    )
    errors = fields.Nested(
        TransferErrorSchema(many=True),
        missing=[],
        dump_only=True,
        description='The value of the `errors` field in the created/updated `Transfer`.'
                    'This field will not be present when the transfer has been deleted, or '
                    'when the field is not present in the created/updated transfer.',
    )


class CreditorUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'CreditorUpdate',
        required=True,
        type='string',
        description='The type of this object.',
        example='CreditorUpdate',
    )
