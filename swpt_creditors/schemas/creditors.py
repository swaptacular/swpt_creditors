from marshmallow import Schema, fields, validate
from flask import url_for
from .common import (
    ObjectReferenceSchema, PaginatedListSchema, MutableResourceSchema, URI_DESCRIPTION,
    MAX_UINT64, PAGE_NEXT_DESCRIPTION, PAGE_FORTHCOMING_DESCRIPTION,
)


class CreditorCreationRequestSchema(Schema):
    type = fields.String(
        missing='CreditorCreationRequest',
        description='The type of this object.',
        example='CreditorCreationRequest',
    )


class CreditorSchema(MutableResourceSchema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/',
    )
    type = fields.Function(
        lambda obj: 'Creditor',
        required=True,
        type='string',
        description='The type of this object.',
        example='Creditor',
    )
    is_active = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='active',
        description="Whether the creditor is currently active."
    )
    created_at_date = fields.Date(
        required=True,
        dump_only=True,
        data_key='createdOn',
        description='The date on which the creditor was created.',
        example='2019-11-30',
    )

    def get_uri(self, obj):
        return url_for(self.context['Creditor'], creditorId=obj.creditor_id)


class WalletSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/wallet',
    )
    type = fields.Function(
        lambda obj: 'Wallet',
        required=True,
        type='string',
        description='The type of this object.',
        example='Wallet',
    )
    creditor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the `Creditor`.",
        example={'uri': '/creditors/2/'},
    )
    accounts = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A `PaginatedList` of `ObjectReference`s to all `Account`s belonging to the '
                    'creditor. The paginated list will not be sorted in any particular order.',
        example={
            'first': '/creditors/2/accounts/',
            'itemsType': 'ObjectReference',
            'type': 'PaginatedList',
        },
    )
    log = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description="A `PaginatedList` of `LogEntry`s. The paginated list will be sorted "
                    "in chronological order (smaller entry IDs go first). This allows the "
                    "clients of the API to synchronize their data by looking at the \"log\".",
        example={
            'first': '/creditors/2/log',
            'forthcoming': '/creditors/2/log?prev=12345',
            'itemsType': 'LogEntry',
            'type': 'PaginatedList',
        },
    )
    transfers = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A `PaginatedList` of `ObjectReference`s to all `Transfer`s initiated '
                    'by the creditor, that have not been deleted yet. The paginated list will '
                    'not be sorted in any particular order.',
        example={
            'first': '/creditors/2/transfers/',
            'itemsType': 'ObjectReference',
            'type': 'PaginatedList',
        },
    )
    createAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='A URI to which a `Debtor` object can be POST-ed to create a new `Account`.',
        example={'uri': '/creditors/2/accounts/'},
    )
    createTransfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='A URI to which a `TransferCreationRequest` can be POST-ed to '
                    'create a new `Transfer`.',
        example={'uri': '/creditors/2/transfers/'},
    )
    accountLookup = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="A URI to which the recipient account's `AccountIdentity` can be POST-ed, "
                    "trying to find a matching sender account. If a matching sender account "
                    "is found, the response will contain an `ObjectReference` to the "
                    "`Account`. Otherwise, the response will be empty (response code 204).",
        example={'uri': '/creditors/2/account-lookup'},
    )
    debtorLookup = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="A URI to which a `Debtor` information can be POST-ed, trying to find an "
                    "existing account with this debtor. If an existing account is found, the "
                    "response will contain an `ObjectReference` to the `Account`. Otherwise, "
                    "the response will be empty (response code 204).",
        example={'uri': '/creditors/2/debtor-lookup'},
    )

    def get_uri(self, obj):
        return url_for(self.context['Wallet'], creditorId=obj.creditor_id)


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
