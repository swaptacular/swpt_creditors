from copy import copy
from marshmallow import Schema, fields, validate, pre_dump, post_dump
from swpt_creditors import models
from swpt_creditors.models import MAX_INT64
from .common import (
    ObjectReferenceSchema, PaginatedListSchema, PaginatedStreamSchema, MutableResourceSchema,
    ValidateTypeMixin, URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION,
)


class CreditorCreationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='CreditorCreationRequest',
        load_only=True,
        description='The type of this object.',
        example='CreditorCreationRequest',
    )
    activate = fields.Boolean(
        missing=False,
        load_only=True,
        description='Whether the creditor must be activated immediately after its creation.',
    )


class CreditorSchema(ValidateTypeMixin, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/',
    )
    type = fields.String(
        missing='Creditor',
        default='Creditor',
        description='The type of this object.',
    )
    created_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the creditor was created.',
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        obj = copy(obj)
        obj.uri = self.context['paths'].creditor(creditorId=obj.creditor_id)
        obj.latest_update_id = obj.creditor_latest_update_id
        obj.latest_update_ts = obj.creditor_latest_update_ts

        return obj


class AccountListSchema(PaginatedListSchema, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/account-list',
    )
    type = fields.Function(
        lambda obj: 'AccountList',
        required=True,
        type='string',
        description='The type of this object.',
        example='AccountList',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet` that contains the account list.",
        example={'uri': '/creditors/2/wallet'},
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.account_list(creditorId=obj.creditor_id)
        obj.wallet = {'uri': paths.wallet(creditorId=obj.creditor_id)}
        obj.first = paths.accounts(creditorId=obj.creditor_id)
        obj.items_type = 'ObjectReference'
        obj.latest_update_id = obj.account_list_latest_update_id
        obj.latest_update_ts = obj.account_list_latest_update_ts

        return obj


class TransferListSchema(PaginatedListSchema, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/transfer-list',
    )
    type = fields.Function(
        lambda obj: 'TransferList',
        required=True,
        type='string',
        description='The type of this object.',
        example='TransferList',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet` that contains the transfer list.",
        example={'uri': '/creditors/2/wallet'},
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.transfer_list(creditorId=obj.creditor_id)
        obj.wallet = {'uri': paths.wallet(creditorId=obj.creditor_id)}
        obj.first = paths.transfers(creditorId=obj.creditor_id)
        obj.items_type = 'ObjectReference'
        obj.latest_update_id = obj.transfer_list_latest_update_id
        obj.latest_update_ts = obj.transfer_list_latest_update_ts

        return obj


class WalletSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
    account_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='accountList',
        description="The URI of creditor's `AccountList`. That is: an URI of a `PaginatedList` of "
                    "`ObjectReference`s to all `Account`s belonging to the creditor. The paginated "
                    "list will not be sorted in any particular order.",
        example={'uri': '/creditors/2/account-list'},
    )
    log = fields.Nested(
        PaginatedStreamSchema,
        required=True,
        dump_only=True,
        description="A `PaginatedStream` of creditor's `LogEntry`s. The paginated stream will be "
                    "sorted in chronological order (smaller entry IDs go first). Normally, the "
                    "log entries will constitute a singly linked list, each entry (except the most "
                    "ancient one) referring to its ancestor. The main purpose of the log stream "
                    "is to allow the clients of the API to reliably and efficiently invalidate "
                    "their caches, simply by following the \"log\".",
        example={
            'first': '/creditors/2/log',
            'forthcoming': '/creditors/2/log?prev=12345',
            'itemsType': 'LogEntry',
            'type': 'PaginatedStream',
        },
    )
    transfer_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='transferList',
        description="The URI of creditor's `TransferList`. That is: an URI of a `PaginatedList` of "
                    "`ObjectReference`s to all `Transfer`s initiated by the creditor, which have not "
                    "been deleted yet. The paginated list will not be sorted in any particular order.",
        example={'uri': '/creditors/2/transfer-list'},
    )
    create_account = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='createAccount',
        description='A URI to which a `DebtorIdentity` object can be POST-ed to create a new `Account`.',
        example={'uri': '/creditors/2/accounts/'},
    )
    create_transfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='createTransfer',
        description='A URI to which a `TransferCreationRequest` can be POST-ed to '
                    'create a new `Transfer`.',
        example={'uri': '/creditors/2/transfers/'},
    )
    account_lookup = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='accountLookup',
        description="A URI to which the recipient account's `AccountIdentity` can be POST-ed, "
                    "trying to find a matching sender account. If a matching sender account "
                    "is found, the response will contain an `ObjectReference` to the "
                    "`Account`. Otherwise, the response will be empty (response code 204).",
        example={'uri': '/creditors/2/account-lookup'},
    )
    debtor_lookup = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='debtorLookup',
        description="A URI to which a `DebtorIdentity` object can be POST-ed, trying to find an "
                    "existing account with this debtor. If an existing account is found, the "
                    "response will contain an `ObjectReference` to the `Account`. Otherwise, "
                    "the response will be empty (response code 204).",
        example={'uri': '/creditors/2/debtor-lookup'},
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.wallet(creditorId=obj.creditor_id)
        obj.creditor = {'uri': paths.creditor(creditorId=obj.creditor_id)}
        obj.account_list = {'uri': paths.account_list(creditorId=obj.creditor_id)}
        obj.transfer_list = {'uri': paths.transfer_list(creditorId=obj.creditor_id)}
        obj.account_lookup = {'uri': paths.account_lookup(creditorId=obj.creditor_id)}
        obj.debtor_lookup = {'uri': paths.debtor_lookup(creditorId=obj.creditor_id)}
        obj.create_account = {'uri': paths.accounts(creditorId=obj.creditor_id)}
        obj.create_transfer = {'uri': paths.transfers(creditorId=obj.creditor_id)}
        log_path = paths.log_entries(creditorId=obj.creditor_id)
        obj.log = {
            'items_type': 'LogEntry',
            'first': log_path,
            'forthcoming': f'{log_path}?prev={obj.latest_log_entry_id}',
        }

        return obj


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
        format='int64',
        data_key='entryId',
        description='The ID of this log entry. This will always be a positive number. Later '
                    'log entries have bigger IDs.',
        example=12345,
    )
    optional_previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        format='int64',
        description="The `entryId` of the previous `LogEntry` for the creditor. Previous "
                    "log entries have smaller IDs. When this field is not present, this "
                    "means that the entry is the first log entry for the creditor.",
        example=12344,
    )
    added_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='addedAt',
        description='The moment at which the entry was added to the log.',
    )
    object_type = fields.String(
        required=True,
        dump_only=True,
        data_key='objectType',
        description='The type of the object that has been created, updated, or deleted.',
        example='Account',
    )
    object = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='The URI of the object that has been created, updated, or deleted.',
        example={'uri': '/creditors/2/accounts/1/'},
    )
    optional_object_update_id = fields.Integer(
        dump_only=True,
        data_key='objectUpdateId',
        description='A positive number which gets bigger after each change in the object. When '
                    'this field is not present, this means that the changed object does not '
                    'have an update ID in it its new state (for example, the object may have '
                    'been deleted, or could be immutable).',
        example=10,
    )
    deleted = fields.Boolean(
        missing=False,
        dump_only=True,
        description='Whether the object has been deleted.',
    )
    optional_data = fields.Dict(
        missing={},
        dump_only=True,
        data_key='data',
        description='Information about the new state of the created/updated object. Generally, '
                    'what data is being provided depends on the specified `objectType`. The '
                    'data can be used so as to avoid making a network request to obtain the '
                    'new state. This field will not be present when the object has been deleted.',
    )

    @pre_dump
    def process_log_entry_instance(self, obj, many):
        assert isinstance(obj, models.LogEntry)
        obj = copy(obj)
        obj.object = {'uri': obj.object_uri}

        if obj.is_deleted:
            obj.deleted = True

        if obj.object_update_id is not None:
            obj.optional_object_update_id = obj.object_update_id

        if obj.previous_entry_id > 0:
            obj.optional_previous_entry_id = obj.previous_entry_id

        if isinstance(obj.data, dict) and not obj.is_deleted:
            obj.optional_data = obj.data

        return obj


class LogPaginationParamsSchema(Schema):
    prev = fields.Integer(
        missing=0,
        load_only=True,
        validate=validate.Range(min=0, max=MAX_INT64),
        format='int64',
        description='Start with the item that follows the item with this index.',
        example=1,
    )


class LogEntriesPageSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
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
    next = fields.String(
        dump_only=True,
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='LogEntriesPage'),
        example='?prev=12345',
    )
    forthcoming = fields.String(
        dump_only=True,
        format='uri-reference',
        description='An URI of another `LogEntriesPage` object which would contain items that '
                    'might be added in the future. That is: items that are not currently available, '
                    'but may become available in the future. This is useful when we want to follow '
                    'a continuous stream of log entries. This field will not be present if, and '
                    'only if, the `next` field is present. This can be a relative URI.',
        example='?prev=12345',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'items' in obj
        assert 'next' in obj or 'forthcoming' in obj
        assert not ('next' in obj and 'forthcoming' in obj)

        return obj
