from marshmallow import Schema, fields
from flask import url_for
from .common import ObjectReferenceSchema, PaginatedListSchema, URI_DESCRIPTION


class CreditorCreationRequestSchema(Schema):
    type = fields.String(
        missing='CreditorCreationRequest',
        description='The type of this object.',
        example='CreditorCreationRequest',
    )


class CreditorSchema(Schema):
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
        dump_only=True,
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


class PortfolioSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/2/portfolio',
    )
    type = fields.Function(
        lambda obj: 'Portfolio',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Portfolio',
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
            'totalItems': 20,
            'first': '/creditors/2/accounts/',
            'itemsType': 'ObjectReference',
            'type': 'PaginatedList',
        },
    )
    log = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description="A `PaginatedList` of recently posted `LogEntry`s. The paginated list will "
                    "be sorted in chronological order (smaller entry IDs go first). This allows "
                    "the clients of the API to synchronize their data by looking at the \"log\".",
        example={
            'first': '/creditors/2/log',
            'forthcoming': '/creditors/2/log?prev=1234567890',
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
            'totalItems': 5,
            'first': '/creditors/2/transfers/',
            'itemsType': 'ObjectReference',
            'type': 'PaginatedList',
        },
    )
    createAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='A URI to which an `AccountCreationRequest` can be POST-ed to '
                    'create a new `Account`.',
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
    findAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="A URI to which the recipient account's `AccountInfo` can be POST-ed, "
                    "trying to find a matching sender `Account`.",
        example={'uri': '/creditors/2/find-account'},
    )

    def get_uri(self, obj):
        return url_for(self.context['Portfolio'], creditorId=obj.creditor_id)
