from marshmallow import Schema, fields
from flask import url_for
from .common import ObjectReferenceSchema, URI_DESCRIPTION
from .paginated_lists import PaginatedListSchema


class CreditorCreationOptionsSchema(Schema):
    pass


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
        missing=True,
        dump_only=True,
        data_key='isActive',
        description="Whether the creditor is currently active or not."
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
        description="The creditor's URI.",
        example={'uri': '/creditors/2/'},
    )
    accountRecords = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A paginated list of references to all account records belonging to the creditor.',
        example={
            'totalItems': 20,
            'first': '/creditors/2/accounts/',
            'itemsType': 'ObjectReference',
            'type': 'PaginatedList',
        },
    )
    journal = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description="A paginated list of recently posted ledger entries (for any of creditor's "
                    "accounts). The paginated list will be sorted in chronological order "
                    "(smaller entry IDs go first). This allows creditors to update the "
                    "position of all their accounts, simply by looking at the \"journal\".",
        example={
            'first': '/creditors/2/journal',
            'forthcoming': '/creditors/2/journal?prev=1234567890',
            'itemsType': 'LedgerEntry',
            'type': 'PaginatedList',
        },
    )
    log = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description="A paginated list of recently posted messages. The paginated list will "
                    "be sorted in chronological order (smaller message IDs go first). This allows "
                    "creditors to obtain the new messages, simply by looking at the \"log\".",
        example={
            'first': '/creditors/2/log',
            'forthcoming': '/creditors/2/log?prev=1234567890',
            'itemsType': 'Message',
            'type': 'PaginatedList',
        },
    )
    directTransfers = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A paginated list of references to for all direct transfers initiated by '
                    'the creditor, that have not been deleted yet. The paginated list will not '
                    'be sorted in any particular order.',
        example={
            'totalItems': 5,
            'first': '/creditors/2/transfers/',
            'itemsType': 'ObjectReference',
            'type': 'PaginatedList',
        },
    )

    def get_uri(self, obj):
        return url_for(self.context['Portfolio'], creditorId=obj.creditor_id)
