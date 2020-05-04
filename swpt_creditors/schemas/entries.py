from marshmallow import fields
from .common import ObjectReferenceSchema, LogEntrySchema, TransferStatusSchema


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
        lambda obj: 'AccountStautsUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountStautsUpdate',
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
