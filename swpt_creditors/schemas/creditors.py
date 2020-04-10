from marshmallow import Schema, fields
from flask import url_for
from swpt_lib import endpoints
from .paginated_lists import PaginatedListSchema


class CreditorCreationOptionsSchema(Schema):
    pass


class CreditorSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/1/',
    )
    type = fields.Function(
        lambda obj: 'Creditor',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Creditor',
    )
    created_at_date = fields.Date(
        required=True,
        dump_only=True,
        data_key='createdOn',
        description='The date on which the creditor was created.',
        example='2019-11-30',
    )
    is_active = fields.Boolean(
        required=True,
        dump_only=True,
        data_key='isActive',
        description="Whether the creditor is currently active or not."
    )

    def get_uri(self, obj):
        return url_for(self.context['Creditor'], _external=True, creditorId=obj.creditor_id)


class PortfolioSchema(Schema):
    uri = fields.Method(
        'get_uri',
        required=True,
        type='string',
        format='uri',
        description="The URI of this object.",
        example='https://example.com/creditors/2/portfolio',
    )
    type = fields.Function(
        lambda obj: 'Portfolio',
        required=True,
        dump_only=True,
        type='string',
        description='The type of this object.',
        example='Portfolio',
    )
    creditorUri = fields.Function(
        lambda obj: endpoints.build_url('creditor', creditorId=obj.creditor_id),
        required=True,
        type='string',
        format="uri",
        description="The creditor's URI.",
        example='https://example.com/creditors/2/',
    )
    accountRecords = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A paginated list of relative URIs for all account records belonging to '
                    'the creditor.',
        example={
            'totalItems': 20,
            'first': 'https://example.com/creditors/2/accounts/',
            'itemsType': 'string',
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
            'first': 'https://example.com/creditors/2/journal',
            'forthcoming': 'https://example.com/creditors/2/journal?prev=1234567890',
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
            'first': 'https://example.com/creditors/2/log',
            'forthcoming': 'https://example.com/creditors/2/log?prev=1234567890',
            'itemsType': 'Message',
            'type': 'PaginatedList',
        },
    )
    directTransfers = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A paginated list of relative URIs for all direct transfers initiated by '
                    'the creditor, that have not been deleted yet. The paginated list will not '
                    'be sorted in any particular order.',
        example={
            'totalItems': 5,
            'first': 'https://example.com/creditors/2/transfers/',
            'itemsType': 'string',
            'type': 'PaginatedList',
        },
    )

    def get_uri(self, obj):
        return url_for(self.context['Portfolio'], _external=True, creditorId=obj.creditor_id)
