from marshmallow import Schema, fields, validate
from .common import (
    ObjectReferenceSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION, PAGE_FORTHCOMING_DESCRIPTION,
)


class LogEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LogEntry',
        required=True,
        type='string',
        description='The type of this object.',
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
    added_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='addedAt',
        description='The moment at which the entry was added to the log.',
    )
    objectType = fields.String(
        required=True,
        dump_only=True,
        description='The type of the object that has been created, updated, or deleted.',
        example='AccountInfo',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the object that has been created, updated, or deleted.',
        example={'uri': '/objects/123'},
    )
    deleted = fields.Boolean(
        missing=False,
        dump_only=True,
        description='Whether the object has been deleted.',
    )
    data = fields.Dict(
        dump_only=True,
        description='Optional information about the new state of the created/updated '
                    'object. It can be used so as to avoid making a network request '
                    'to obtain the new state. This field will not be present when '
                    'the object has been deleted.',
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
        example='?prev=12345',
    )


class LedgerEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LedgerEntry',
        required=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntry',
    )
    ledger = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the corresponding `AccountLedger`.',
        example={'uri': '/creditors/2/accounts/1/ledger'},
    )
    entry_id = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format='uint64',
        data_key='entryId',
        description='The ID of the ledger entry. Later ledger entries have bigger IDs. Note '
                    'that those IDs are the same as the IDs of the `LogEntry`s added to '
                    'the log for the corresponding `AccountLedger`.',
        example=12345,
    )
    added_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='addedAt',
        description='The moment at which the entry was added to the ledger.',
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
        description="The `entryId` of the previous `LedgerEntry` for this account. Previous "
                    "entries have smaller IDs. When this field is not present, this means "
                    "that there are no previous entries in the account's ledger.",
        example=122,
    )


class LedgerEntriesPageSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/1/entries?prev=124',
    )
    type = fields.Function(
        lambda obj: 'LedgerEntriesPage',
        required=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntriesPage',
    )
    items = fields.Nested(
        LedgerEntrySchema(many=True),
        required=True,
        dump_only=True,
        description='An array of `LedgerEntry`s. Can be empty.',
    )
    next = fields.Method(
        'get_next_uri',
        type='string',
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='LedgerEntriesPage'),
    )
