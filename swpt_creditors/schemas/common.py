from marshmallow import Schema, fields, validate, validates, post_dump, ValidationError
from swpt_creditors.models import MAX_INT64

URI_DESCRIPTION = '\
The URI of this object. Can be a relative URI.'

PAGE_NEXT_DESCRIPTION = '\
An URI of another `{type}` object which contains more items. When \
there are no remaining items, this field will not be present. If this field \
is present, there might be remaining items, even when the `items` array is \
empty. This can be a relative URI.'


class type_registry:
    paginated_list = 'PaginatedList'
    paginated_stream = 'PaginatedStream'
    object_references_page = 'ObjectReferencesPage'
    object_reference = 'ObjectReference'
    account_identity = 'AccountIdentity'
    debtor_identity = 'DebtorIdentity'
    debtor_info = 'DebtorInfo'
    currency_peg = 'CurrencyPeg'
    wallet = 'Wallet'
    creditor_reservation_request = 'CreditorReservationRequest'
    creditor_reservation = 'CreditorReservation'
    creditor_activation_request = 'CreditorActivationRequest'
    creditor_deactivation_request = 'CreditorDeactivationRequest'
    creditor_creation_request = 'CreditorCreationRequest'
    creditor = 'Creditor'
    log_entries_page = 'LogEntriesPage'
    log_entry = 'LogEntry'
    accounts_list = 'AccountsList'
    account = 'Account'
    account_info = 'AccountInfo'
    account_config = 'AccountConfig'
    account_display = 'AccountDisplay'
    account_exchange = 'AccountExchange'
    account_knowledge = 'AccountKnowledge'
    account_ledger = 'AccountLedger'
    ledger_entries_page = 'LedgerEntriesPage'
    ledger_entry = 'LedgerEntry'
    transfers_list = 'TransfersList'
    transfer_creation_request = 'TransferCreationRequest'
    transfer_cancelation_request = 'TransferCancelationRequest'
    transfer = 'Transfer'
    transfer_options = 'TransferOptions'
    transfer_result = 'TransferResult'
    transfer_error = 'TransferError'
    committed_transfer = 'CommittedTransfer'


class ValidateTypeMixin:
    @validates('type')
    def validate_type(self, value):
        if f'{value}Schema' != type(self).__name__:
            raise ValidationError('Invalid type.')


class PaginationParametersSchema(Schema):
    prev = fields.String(
        load_only=True,
        description='Start with the item that follows the item with this index.',
        example='1',
    )
    stop = fields.String(
        load_only=True,
        description='Return only items which precedes the item with this index.',
        example='100',
    )


class PaginatedListSchema(Schema):
    type = fields.Function(
        lambda obj: type_registry.paginated_list,
        required=True,
        type='string',
        description='The type of this object.',
        example='PaginatedList',
    )
    items_type = fields.String(
        required=True,
        dump_only=True,
        data_key='itemsType',
        description='The type of the items in the paginated list.',
        example='string',
    )
    first = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description='The URI of the first page in the paginated list. This can be a relative URI. '
                    'The object retrieved from this URI will have: 1) An `items` field (an '
                    'array), which will contain the first items of the paginated list; 2) May '
                    'have a `next` field (a string), which would contain the URI of the next '
                    'page in the list.',
        example='/list?page=1',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'itemsType' in obj
        assert 'first' in obj
        return obj


class PaginatedStreamSchema(Schema):
    type = fields.Function(
        lambda obj: type_registry.paginated_stream,
        required=True,
        type='string',
        description='The type of this object.',
        example='PaginatedStream',
    )
    items_type = fields.String(
        required=True,
        dump_only=True,
        data_key='itemsType',
        description='The type of the items in the paginated stream.',
        example='string',
    )
    first = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description='The URI of the first page in the paginated stream. This can be a relative '
                    'URI. The object retrieved from this URI will have: 1) An `items` field (an '
                    'array), which will contain the first items of the paginated stream; 2) May '
                    'have a `next` field (a string), which would contain the URI of the next '
                    'page in the stream; 3) If the `next` field is not present, will have a '
                    '`forthcoming` field, for obtaining items that might be added to the stream '
                    'in the future.',
        example='/stream?page=1',
    )
    forthcoming = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description='An URI for obtaining items that might be added to the paginated stream in the '
                    'future. This is useful when the client wants to skip all items currently in the '
                    'stream, but to follow the forthcoming stream of new items. The object retrieved '
                    'from this URI will be of the same type as the one retrieved from the `first` '
                    'field. This can be a relative URI.',
        example='/stream?page=1000',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'itemsType' in obj
        assert 'first' in obj
        assert 'forthcoming' in obj
        return obj


class ObjectReferenceSchema(Schema):
    uri = fields.String(
        required=True,
        format='uri-reference',
        description="The URI of the object. Can be a relative URI.",
        example='https://example.com/objects/1',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        return obj


class ObjectReferencesPageSchema(Schema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/accounts/',
    )
    type = fields.Function(
        lambda obj: type_registry.object_references_page,
        required=True,
        type='string',
        description='The type of this object.',
        example='ObjectReferencesPage',
    )
    items = fields.Nested(
        ObjectReferenceSchema(many=True),
        required=True,
        dump_only=True,
        description='An array of `ObjectReference`s. Can be empty.',
        example=[{'uri': f'{i}/'} for i in [1, 11, 111]],
    )
    next = fields.String(
        dump_only=True,
        format='uri-reference',
        description=PAGE_NEXT_DESCRIPTION.format(type='ObjectReferencesPage'),
        example='?prev=111',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'items' in obj
        return obj


class AccountIdentitySchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing=type_registry.account_identity,
        default=type_registry.account_identity,
        description='The type of this object.',
        example='AccountIdentity',
    )
    uri = fields.String(
        required=True,
        validate=validate.Length(max=200),
        format='uri',
        description="The URI of the account. The information contained in the URI must be "
                    "enough to: 1) uniquely and reliably identify the debtor, 2) uniquely "
                    "and reliably identify the creditor's account with the debtor. Note that "
                    "a network request *should not be needed* to identify the account. "
                    "\n\n"
                    "For example, if the debtor happens to be a bank, the URI would reveal "
                    "the type of the debtor (a bank), the ID of the bank, and the bank "
                    "account number.",
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        return obj


class MutableResourceSchema(Schema):
    latest_update_id = fields.Integer(
        required=True,
        validate=validate.Range(min=1, max=MAX_INT64),
        data_key='latestUpdateId',
        format='int64',
        description='The sequential number of the latest update in the object. This will always '
                    'be a positive number, which starts from `1` and gets incremented after each '
                    'change in the object.'
                    '\n\n'
                    'When the object is changed by the server, the value of this field will be '
                    'incremented automatically, and will be equal to the value of the '
                    '`objectUpdateId` field in the latest `LogEntry` for this object in the '
                    'log. In this case, the value of the field can be used by the client to decide '
                    'whether a network request should made to obtain the newest state of the '
                    'object.'
                    '\n\n'
                    'When the object is changed by the client, the value of this field must be '
                    'incremented by the client. In this case, the server will use the value of the '
                    'field to detect conflicts which can occur when two clients try to update the '
                    'object simultaneously.',
        example=123,
    )
    latest_update_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='latestUpdateAt',
        description='The moment of the latest update on this object. The value is the same as '
                    'the value of the `addedAt` field in the latest `LogEntry` for this object '
                    'in the log.',
    )
