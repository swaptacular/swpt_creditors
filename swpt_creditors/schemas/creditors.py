from copy import copy
from marshmallow import Schema, fields, validate, pre_dump, post_dump
from swpt_creditors import models
from swpt_creditors.models import MAX_INT64
from .common import (
    ObjectReferenceSchema, PaginatedListSchema, MutableResourceSchema, ValidateTypeMixin,
    URI_DESCRIPTION, PAGE_NEXT_DESCRIPTION, PAGE_FORTHCOMING_DESCRIPTION,
)


class CreditorCreationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_only=True,
        missing='CreditorCreationRequest',
        description='The type of this object.',
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
    is_active = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='active',
        description='Whether the creditor is active. The creditor becomes active after the first '
                    'write operation (for example, account or transfer creation).',
    )
    created_at_date = fields.Date(
        required=True,
        dump_only=True,
        data_key='createdOn',
        description='The date on which the creditor was created.',
        example='2019-11-30',
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
    class Meta:
        exclude = ['forthcoming']

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
    class Meta:
        exclude = ['forthcoming']

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
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description="A `PaginatedList` of creditor's `LogEntry`s. The paginated list will be "
                    "sorted in chronological order (smaller entry IDs go first). The entries "
                    "will constitute a singly linked list, each entry (except the most ancient "
                    "one) referring to its ancestor. Also, this is a \"streaming\" paginated "
                    "list (the `forthcoming` field will be present), allowing the clients "
                    "of the API to reliably and efficiently invalidate their caches, simply "
                    "by following the \"log\" .",
        example={
            'first': '/creditors/2/log',
            'forthcoming': '/creditors/2/log?prev=12345',
            'itemsType': 'LogEntry',
            'type': 'PaginatedList',
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
        validate=validate.Range(min=1, max=MAX_INT64),
        format='int64',
        data_key='entryId',
        description='The ID of this log entry. Later log entries have bigger IDs. This '
                    'will always be a positive number.',
        example=12345,
    )
    optional_previous_entry_id = fields.Integer(
        dump_only=True,
        data_key='previousEntryId',
        validate=validate.Range(min=1, max=MAX_INT64),
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

        if obj.previous_entry_id is not None:
            obj.optional_previous_entry_id = obj.previous_entry_id

        if isinstance(obj.data, dict) and not obj.is_deleted:
            obj.optional_data = obj.data

        return obj


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
        description=PAGE_FORTHCOMING_DESCRIPTION.format(type='LogEntriesPage'),
        example='?prev=12345',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'items' in obj
        assert 'next' in obj or 'forthcoming' in obj
        assert not ('next' in obj and 'forthcoming' in obj)

        return obj
