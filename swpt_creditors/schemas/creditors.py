from marshmallow import Schema, fields
from flask import url_for
from .common import ObjectReferenceSchema, PaginatedListSchema, URI_DESCRIPTION


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
        required=True,
        dump_only=True,
        data_key='isActive',
        description="Whether the creditor is currently active or not."
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
        description="The creditor's URI.",
        example={'uri': '/creditors/2/'},
    )
    accounts = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A paginated list of references to all accounts belonging to the creditor.',
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
    transfers = fields.Nested(
        PaginatedListSchema,
        required=True,
        dump_only=True,
        description='A paginated list of references to for all transfers initiated by the '
                    'creditor, that have not been deleted yet. The paginated list will not '
                    'be sorted in any particular order.',
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
        description='A URI to which an `AccountCreationRequest` can be POST-ed, '
                    'trying to create a new `Account`.',
        example={'uri': '/creditors/2/accounts/'},
    )
    createTransfer = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description='A URI to which a `TransferCreationRequest` can be POST-ed, '
                    'trying to create a new `Transfer`.',
        example={'uri': '/creditors/2/transfers/'},
    )
    findAccount = fields.Nested(
        ObjectReferenceSchema,
        required=True,
        dump_only=True,
        description="A URI to which recipient's account `AccountInfo` can be POST-ed, "
                    "trying to find a matching sender `Account`.",
        example={'uri': '/creditors/2/find-account'},
    )

    def get_uri(self, obj):
        return url_for(self.context['Portfolio'], creditorId=obj.creditor_id)
