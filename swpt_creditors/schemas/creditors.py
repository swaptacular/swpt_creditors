from copy import copy
from marshmallow import Schema, ValidationError, fields, validate, validates_schema, pre_dump, post_dump
from swpt_creditors import models
from swpt_creditors.models import MAX_INT64, PinInfo, PIN_REGEX
from .common import ObjectReferenceSchema, PaginatedListSchema, PaginatedStreamSchema, \
    MutableResourceSchema, PinProtectedResourceSchema, type_registry, ValidateTypeMixin, URI_DESCRIPTION, \
    PAGE_NEXT_DESCRIPTION, TYPE_DESCRIPTION


class CreditorSchema(ValidateTypeMixin, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/',
    )
    type = fields.String(
        missing=type_registry.creditor,
        default=type_registry.creditor,
        description=TYPE_DESCRIPTION,
        example='Creditor',
    )
    created_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the creditor was created.',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet`.",
        example={'uri': '/creditors/2/wallet'},
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.creditor(creditorId=obj.creditor_id)
        obj.wallet = {'uri': paths.wallet(creditorId=obj.creditor_id)}
        obj.latest_update_id = obj.creditor_latest_update_id
        obj.latest_update_ts = obj.creditor_latest_update_ts

        return obj


class PinInfoSchema(ValidateTypeMixin, MutableResourceSchema, PinProtectedResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/pin',
    )
    type = fields.String(
        missing=type_registry.pin_info,
        default=type_registry.pin_info,
        description=TYPE_DESCRIPTION,
        example='PinInfo',
    )
    status_name = fields.String(
        required=True,
        validate=validate.Regexp(f'^({"|".join(PinInfo.STATUS_NAMES)})$'),
        data_key='status',
        description='The status of the PIN.'
                    '\n\n'
                    '* `"off"` means that the PIN is not required for potentially '
                    '  dangerous operations.\n'
                    '* `"on"` means that the PIN is required for potentially dangerous '
                    '  operations.\n'
                    '* `"blocked"` means that the PIN has been blocked.',
        example=f'{PinInfo.STATUS_NAME_ON}',
    )
    optional_new_pin_value = fields.String(
        load_only=True,
        validate=validate.Regexp(PIN_REGEX),
        data_key='newPin',
        description='The new PIN. When `status` is "on", this field must be present. Note '
                    'that when changing the PIN, the `pin` field should contain the old '
                    'PIN, and the `newPin` field should contain the new PIN.',
        example='5678',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet`.",
        example={'uri': '/creditors/2/wallet'},
    )

    @validates_schema
    def validate_value(self, data, **kwargs):
        is_on = data['status_name'] == PinInfo.STATUS_NAME_ON
        if is_on and 'optional_new_pin_value' not in data:
            raise ValidationError('When the PIN is "on", newPin is requred.')

    @pre_dump
    def process_pin_instance(self, obj, many):
        assert isinstance(obj, models.PinInfo)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.pin_info(creditorId=obj.creditor_id)
        obj.wallet = {'uri': paths.wallet(creditorId=obj.creditor_id)}
        obj.latest_update_id = obj.latest_update_id
        obj.latest_update_ts = obj.latest_update_ts

        return obj


class AccountsListSchema(PaginatedListSchema, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts-list',
    )
    type = fields.Function(
        lambda obj: type_registry.accounts_list,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
        example='AccountsList',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet` that contains the accounts list.",
        example={'uri': '/creditors/2/wallet'},
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.accounts_list(creditorId=obj.creditor_id)
        obj.wallet = {'uri': paths.wallet(creditorId=obj.creditor_id)}
        obj.first = paths.accounts(creditorId=obj.creditor_id)
        obj.items_type = type_registry.object_reference
        obj.latest_update_id = obj.accounts_list_latest_update_id
        obj.latest_update_ts = obj.accounts_list_latest_update_ts

        return obj


class TransfersListSchema(PaginatedListSchema, MutableResourceSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/transfers-list',
    )
    type = fields.Function(
        lambda obj: type_registry.transfers_list,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
        example='TransfersList',
    )
    wallet = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the creditor's `Wallet` that contains the transfers list.",
        example={'uri': '/creditors/2/wallet'},
    )

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        obj = copy(obj)
        obj.uri = paths.transfers_list(creditorId=obj.creditor_id)
        obj.wallet = {'uri': paths.wallet(creditorId=obj.creditor_id)}
        obj.first = paths.transfers(creditorId=obj.creditor_id)
        obj.items_type = type_registry.object_reference
        obj.latest_update_id = obj.transfers_list_latest_update_id
        obj.latest_update_ts = obj.transfers_list_latest_update_ts

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
        lambda obj: type_registry.wallet,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
        example='Wallet',
    )
    creditor = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="The URI of the `Creditor`.",
        example={'uri': '/creditors/2/'},
    )
    accounts_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='accountsList',
        description="The URI of creditor's `AccountsList`. That is: an URI of a `PaginatedList` of "
                    "`ObjectReference`s to all `Account`s belonging to the creditor. The paginated "
                    "list will not be sorted in any particular order.",
        example={'uri': '/creditors/2/accounts-list'},
    )
    log = fields.Nested(
        PaginatedStreamSchema,
        required=True,
        dump_only=True,
        description="A `PaginatedStream` of creditor's `LogEntry`s. The paginated stream will be "
                    "sorted in chronological order (smaller entry IDs go first). The main "
                    "purpose of the log stream is to allow the clients of the API to reliably "
                    "and efficiently invalidate their caches, simply by following the \"log\".",
        example={
            'first': '/creditors/2/log',
            'forthcoming': '/creditors/2/log?prev=12345',
            'itemsType': 'LogEntry',
            'type': 'PaginatedStream',
        },
    )
    last_log_entry_id = fields.Integer(
        required=True,
        dump_only=True,
        format="int64",
        data_key='logLatestEntryId',
        description="The ID of the latest entry in the creditor's log stream. If there are "
                    "no entries yet, the value will be `0`.",
        example=12345,
    )
    log_retention_days = fields.Method(
        'get_log_retention_days',
        required=True,
        dump_only=True,
        type='integer',
        format="int32",
        data_key='logRetentionDays',
        description="The entries in the creditor's log stream will not be deleted for at least this "
                    "number of days. Must be at least 30 days.",
        example=30,
    )
    transfers_list = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='transfersList',
        description="The URI of creditor's `TransfersList`. That is: an URI of a `PaginatedList` of "
                    "`ObjectReference`s to all `Transfer`s initiated by the creditor, which have not "
                    "been deleted yet. The paginated list will not be sorted in any particular order.",
        example={'uri': '/creditors/2/transfers-list'},
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
                    "trying to find the identify of the account's debtor. If the debtor has "
                    "been identified successfully, the response will contain the debtor's "
                    "`DebtorIdentity`. Otherwise, the response code will be 422.",
        example={'uri': '/creditors/2/account-lookup'},
    )
    debtor_lookup = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='debtorLookup',
        description="A URI to which a `DebtorIdentity` object can be POST-ed, trying to find an "
                    "existing account with this debtor. If an existing account is found, the "
                    "response will redirect to the `Account` (response code 303). Otherwise, "
                    "the response will be empty (response code 204).",
        example={'uri': '/creditors/2/debtor-lookup'},
    )
    pin_info_reference = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        data_key='pinInfo',
        description="The URI of creditor's `PinInfo`.",
        example={'uri': '/creditors/2/pin'},
    )
    require_pin = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='requirePin',
        description="Whether the PIN is required for potentially dangerous operations."
                    "\n\n"
                    "**Note:** The PIN will never be required when in \"PIN reset\" mode.",
        example=True,
    )

    def get_log_retention_days(self, obj):
        calc_log_retention_days = self.context['calc_log_retention_days']
        days = calc_log_retention_days(obj.creditor_id)
        assert days > 0
        return days

    @pre_dump
    def process_creditor_instance(self, obj, many):
        assert isinstance(obj, models.Creditor)
        paths = self.context['paths']
        calc_require_pin = self.context['calc_require_pin']
        obj = copy(obj)
        obj.uri = paths.wallet(creditorId=obj.creditor_id)
        obj.creditor = {'uri': paths.creditor(creditorId=obj.creditor_id)}
        obj.accounts_list = {'uri': paths.accounts_list(creditorId=obj.creditor_id)}
        obj.transfers_list = {'uri': paths.transfers_list(creditorId=obj.creditor_id)}
        obj.account_lookup = {'uri': paths.account_lookup(creditorId=obj.creditor_id)}
        obj.debtor_lookup = {'uri': paths.debtor_lookup(creditorId=obj.creditor_id)}
        obj.create_account = {'uri': paths.accounts(creditorId=obj.creditor_id)}
        obj.create_transfer = {'uri': paths.transfers(creditorId=obj.creditor_id)}
        obj.pin_info_reference = {'uri': paths.pin_info(creditorId=obj.creditor_id)}
        obj.require_pin = calc_require_pin(obj.pin_info)
        log_path = paths.log_entries(creditorId=obj.creditor_id)
        obj.log = {
            'items_type': type_registry.log_entry,
            'first': log_path,
            'forthcoming': f'{log_path}?prev={obj.last_log_entry_id}',
        }

        return obj


class LogEntrySchema(Schema):
    type = fields.Function(
        lambda obj: type_registry.log_entry,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
        example='LogEntry',
    )
    entry_id = fields.Integer(
        required=True,
        dump_only=True,
        format='int64',
        data_key='entryId',
        description='The ID of the log entry. This will always be a positive number. The first '
                    'log entry has an ID of `1`, and the ID of each subsequent log entry will '
                    'be equal to the ID of the previous log entry plus one.',
        example=12345,
    )
    added_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='addedAt',
        description='The moment at which the entry was added to the log.',
    )
    object_type = fields.Method(
        'get_object_type',
        required=True,
        dump_only=True,
        type='string',
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
    is_deleted = fields.Function(
        lambda obj: bool(obj.is_deleted),
        required=True,
        dump_only=True,
        type='boolean',
        data_key='deleted',
        description='Whether the object has been deleted.',
        example=False,
    )
    optional_object_update_id = fields.Integer(
        dump_only=True,
        data_key='objectUpdateId',
        format='int64',
        description='A positive number which gets incremented after each change in the '
                    'object. When this field is not present, this means that the changed object '
                    'does not have an update ID (the object is immutable, or has been deleted, '
                    'for example).',
        example=10,
    )
    optional_data = fields.Dict(
        dump_only=True,
        data_key='data',
        description='Optional information about the new state of the created/updated object. When '
                    'present, this information can be used to avoid making a network request to '
                    'obtain the new state. What properties the "data" object will have, depends '
                    'on the value of the `objectType` field:'
                    '\n\n'
                    '### When the object type is "AccountLedger"\n'
                    '`principal` and `nextEntryId` properties will  be present.'
                    '\n\n'
                    '### When the object type is "Transfer"\n'
                    'If the transfer is finalized, `finalizedAt` and (only when there is an '
                    'error) `errorCode` properties will be present. If the transfer is not '
                    'finalized, the "data" object will not be present.'
                    '\n\n'
                    '**Note:** This field will never be present when the object has been deleted.',
    )

    @pre_dump
    def process_log_entry_instance(self, obj, many):
        assert isinstance(obj, models.LogEntry)
        obj = copy(obj)
        obj.object = {'uri': obj.get_object_uri(self.context['paths'])}

        if obj.object_update_id is not None:
            obj.optional_object_update_id = obj.object_update_id

        if not obj.is_deleted:
            data = obj.get_data_dict()
            if data is not None:
                obj.optional_data = data

        return obj

    def get_object_type(self, obj):
        return obj.get_object_type(self.context['types'])


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
        lambda obj: type_registry.log_entries_page,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
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
