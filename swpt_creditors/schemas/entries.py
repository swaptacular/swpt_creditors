from marshmallow import fields
from .common import ObjectReferenceSchema, LogEntrySchema


class AccountUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountUpdate',
    )
    account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated account.",
        example={'uri': '/creditors/2/accounts/1/'},
    )
    isDeleted = fields.Boolean(
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
    status = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated account status information.",
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
    config = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated account configuration.",
        example={'uri': '/creditors/2/accounts/1/config'},
    )


class AccountExchangeSettingsUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountExchangeSettingsUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountExchangeSettingsUpdate',
    )
    exchangeSettings = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated account exchange settings.",
        example={'uri': '/creditors/2/accounts/1/exchange'},
    )


class AccountDisplaySettingsUpdateSchema(LogEntrySchema):
    type = fields.Function(
        lambda obj: 'AccountDisplaySettingsUpdate',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='AccountDisplaySettingsUpdate',
    )
    displaySettings = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated account display settings.",
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
    transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the updated transfer.",
        example={'uri': '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000'},
    )
    isDeleted = fields.Boolean(
        dump_only=True,
        missing=False,
        description="Whether the transfer has been deleted.",
    )
