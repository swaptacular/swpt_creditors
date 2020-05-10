from marshmallow import Schema, fields, validate
from .common import (
    ObjectReferenceSchema, TransferStatusSchema,
    MAX_INT64, MAX_UINT64, URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION, PAGE_FORTHCOMING_DESCRIPTION,
)


class LogEntrySchema(Schema):
    type = fields.Function(
        lambda obj: 'LogEntry',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object. Different kinds of log entries may use different '
                    '**additional fields**, providing more data. This field contains the name '
                    'of the used schema.',
        example='LogEntry',
    )
    entryId = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=1, max=MAX_UINT64),
        format='uint64',
        description='The ID of this log entry. Later log entries have bigger IDs.',
        example=12345,
    )
    posted_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='postedAt',
        description='The moment at which this entry was added to the log.',
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
        dump_only=True,
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


class LedgerEntrySchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'LedgerEntry',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='LedgerEntry',
    )
    ledger = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the corresponding `AccountLedger`.",
        example={'uri': '/creditors/2/accounts/1/ledger'},
    )
    postedAmount = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description="The amount added to account's principal. Can be positive (an increase) or "
                    "negative (a decrease). Can not be zero.",
        example=1000,
    )
    principal = fields.Integer(
        required=True,
        dump_only=True,
        validate=validate.Range(min=-MAX_INT64, max=MAX_INT64),
        format='int64',
        description='The new principal amount on the account, as it is after the transfer. Unless '
                    'a principal overflow has occurred, the new principal amount will be equal to '
                    '`amount` plus the old principal amount.',
        example=1500,
    )
    transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the corresponding `CommittedTransfer`.',
        example={'uri': '/creditors/2/accounts/1/transfers/999'},
    )
    previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        validate=validate.Range(min=1, max=MAX_UINT64),
        format='uint64',
        description="The ID of the previous entry in the account's ledger. Previous entries have "
                    "smaller IDs. When this field is not present, this means that there are no "
                    "previous entries in the account's ledger.",
        example=122,
    )
    reference = fields.String(
        dump_only=True,
        missing='',
        description="A payment reference. A payment reference is a short string that can be "
                "included with transfers to help identify the transfer. For incoming transfers "
                "this will be the transfer's `payeeRef` field. For outgoing transfers this "
                "will be the transfer's `payerRef` field.",
        example='PAYMENT 123',
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
        dump_only=True,
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


class AccountUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountUpdate',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated `Account`.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    deleted = fields.Boolean(
        dump_only=True,
        missing=False,
        description="Whether the account has been deleted.",
    )


class AccountStatusUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountStatusUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountStatusUpdate',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated `AccountStatus`.",
        example={'uri': '/creditors/2/accounts/1/status'},
    )


class AccountConfigUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountConfigUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountConfigUpdate',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated `AccountConfig`.",
        example={'uri': '/creditors/2/accounts/1/config'},
    )


class AccountExchangeUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountExchangeUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountExchangeUpdate',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated `AccountExchange` settings.",
        example={'uri': '/creditors/2/accounts/1/exchange'},
    )


class AccountDisplayUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountDisplayUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountDisplayUpdate',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated `AccountDisplay` settings.",
        example={'uri': '/creditors/2/accounts/1/display'},
    )


class TransferUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'TransferUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='TransferUpdate',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated `Transfer`.",
        example={'uri': '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000'},
    )
    status = fields.Nested(
        TransferStatusSchema,
        dump_only=True,
        description='The current `TransferStatus` for the updated transfer. This field '
                    'will not be present when the transfer has been deleted, or when a new '
                    'transfer has been just created. (When a new transfer has been just '
                    'created, we can not avoid making an HTTP request to obtain the whole '
                    '`Transfer` object anyway.)'
    )
    deleted = fields.Boolean(
        dump_only=True,
        missing=False,
        description="Whether the transfer has been deleted.",
    )
