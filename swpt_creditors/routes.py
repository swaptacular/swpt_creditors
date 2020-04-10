from urllib.parse import urljoin
from flask import redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints
from .schemas import (
    CreditorCreationOptionsSchema, CreditorSchema, AccountCreationRequestSchema,
    AccountSchema, AccountRecordSchema, AccountRecordConfigSchema, CommittedTransferSchema,
    LedgerEntriesPage, PortfolioSchema, LinksPage, PaginationParametersSchema, MessagesPageSchema,
    DirectTransferCreationRequestSchema, DirectTransferSchema, DirectTransferUpdateRequestSchema,
)
from .specs import DID, CID, SEQNUM, TRANSFER_UUID
from . import specs
from . import procedures

CONTEXT = {
    'Creditor': 'public.CreditorEndpoint',
    'Account': 'public.AccountEndpoint',
    'Transfer': 'transfers.TransferEndpoint',
    'AccountList': 'accounts.AccountListEndpoint',
    'AccountRecord': 'accounts.AccountRecordEndpoint',
}


public_api = Blueprint(
    'public',
    __name__,
    url_prefix='/creditors',
    description="Obtain public information about creditors and accounts, create new creditors.",
)


@public_api.route('/<i64:creditorId>/', parameters=[CID])
class CreditorEndpoint(MethodView):
    @public_api.response(CreditorSchema(context=CONTEXT))
    @public_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return public information about a creditor."""

        creditor = procedures.get_creditor(creditorId)
        if not creditor:
            abort(404)
        return creditor, {'Cache-Control': 'max-age=86400'}

    @public_api.arguments(CreditorCreationOptionsSchema)
    @public_api.response(CreditorSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @public_api.doc(responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_creation_options, creditorId):
        """Try to create a new creditor. Requires special privileges.

        ---
        Must fail if the creditor already exists.

        """

        try:
            creditor = procedures.create_new_creditor(creditorId)
        except procedures.CreditorExistsError:
            abort(409)
        return creditor, {'Location': endpoints.build_url('creditor', creditorId=creditorId)}


@public_api.route('/<i64:creditorId>/debtors/<i64:debtorId>', parameters=[CID, DID])
class AccountEndpoint(MethodView):
    @public_api.response(AccountSchema(context=CONTEXT))
    @public_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return public information about an account."""

        account = None
        if not account:
            abort(404)
        return account, {'Cache-Control': 'max-age=86400'}


portfolio_api = Blueprint(
    'portfolio',
    __name__,
    url_prefix='/creditors',
    description="View creditors' portfolios.",
)


@portfolio_api.route('/<i64:creditorId>/portfolio', parameters=[CID])
class PortfolioEndpoint(MethodView):
    @portfolio_api.response(PortfolioSchema(context=CONTEXT))
    @portfolio_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return creditor's portfolio."""

        abort(500)


@portfolio_api.route('/<i64:creditorId>/journal', parameters=[CID])
class CreditorJournalEndpoint(MethodView):
    @portfolio_api.arguments(PaginationParametersSchema, location='query')
    @portfolio_api.response(LedgerEntriesPage(context=CONTEXT), example=specs.JOURNAL_LEDGER_ENTRIES_EXAMPLE)
    @portfolio_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of creditor's recently posted ledger entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recently posted ledger
        entries (for any of creditor's accounts). The returned
        fragment will be sorted in chronological order (smaller entry
        IDs go first).

        """

        abort(500)


@portfolio_api.route('/<i64:creditorId>/log', parameters=[CID])
class CreditorLogEndpoint(MethodView):
    @portfolio_api.arguments(PaginationParametersSchema, location='query')
    @portfolio_api.response(MessagesPageSchema(context=CONTEXT), example=specs.JOURNAL_MESSAGES_EXAMPLE)
    @portfolio_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of creditor's recently posted messages.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recently posted
        messages. The returned fragment will be sorted in
        chronological order (smaller message IDs go first).

        """

        abort(500)


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="View, update and delete creditors' account records.",
)


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[CID])
class AccountRecordsEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LinksPage(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of account records belonging to a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains the relative URIs of all
        account records belonging to a given creditor. The returned
        fragment will not be sorted in any particular order.

        """

        try:
            debtor_ids = procedures.get_account_dedtor_ids(creditorId)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        return debtor_ids

    @accounts_api.arguments(AccountCreationRequestSchema)
    @accounts_api.response(AccountRecordSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @accounts_api.doc(responses={303: specs.ACCOUNT_EXISTS,
                                 403: specs.TOO_MANY_ACCOUNTS,
                                 404: specs.CREDITOR_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_CONFLICT,
                                 422: specs.ACCOUNT_CAN_NOT_BE_CREATED})
    def post(self, account_creation_request, creditorId):
        """Create a new account record belonging to a given creditor."""

        debtor_uri = account_creation_request['debtor_uri']
        try:
            debtor_id = endpoints.match_url('debtor', debtor_uri)['debtorId']
            location = url_for(
                'accounts.AccountRecordEndpoint',
                _external=True,
                creditorId=creditorId,
                debtorId=debtor_id,
            )
            transfer = procedures.create_account(creditorId, debtor_id)
        except endpoints.MatchError:
            abort(422)
        except procedures.TooManyManagementActionsError:
            abort(403)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        except procedures.AccountsConflictError:
            abort(409)
        except procedures.AccountExistsError:
            return redirect(location, code=303)
        return transfer, {'Location': location}


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/', parameters=[CID, DID])
class AccountRecordEndpoint(MethodView):
    @accounts_api.response(AccountRecordSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_RECORD_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return an account record."""

        abort(500)

    @accounts_api.response(code=204)
    def delete(self, creditorId, debtorId):
        """Delete an account record.

        **Important note:** If the account record is not marked as
        safe for deletion, deleting it may result in losing a
        non-negligible amount of money on the account.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/config', parameters=[CID, DID])
class AccountRecordConfigEndpoint(MethodView):
    @accounts_api.response(AccountRecordConfigSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_RECORD_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account record's configuration."""

        abort(500)

    @accounts_api.arguments(AccountRecordConfigSchema)
    @accounts_api.response(AccountRecordConfigSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_RECORD_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_UPDATE_CONFLICT})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account record's configuration.

        **Note:** For practical purposes, this operation is can be
        treated as idempotent.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[CID, DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LedgerEntriesPage(context=CONTEXT), example=specs.ACCOUNT_LEDGER_ENTRIES_EXAMPLE)
    @accounts_api.doc(responses={404: specs.ACCOUNT_RECORD_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId, debtorId):
        """Return a collection of ledger entries for a given account record.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recent ledger entries
        for a given account record. The returned fragment will be
        sorted in reverse-chronological order (bigger entry IDs go
        first). The entries will constitute a singly linked list, each
        entry (except the most ancient one) referring to its ancestor.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<i64:seqnum>', parameters=[CID, DID, SEQNUM])
class AccountTransferEndpoint(MethodView):
    @accounts_api.response(CommittedTransferSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId, seqnum):
        """Return information about sent or received transfer."""

        abort(500)


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/creditors',
    description="Make direct transfers from one account to another account.",
)


@transfers_api.route('/<i64:creditorId>/transfers/', parameters=[CID])
class DirectTransfersEndpoint(MethodView):
    @transfers_api.arguments(PaginationParametersSchema, location='query')
    @transfers_api.response(LinksPage(context=CONTEXT), example=specs.DIRECT_TRANSFER_LINKS_EXAMPLE)
    @transfers_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of direct transfers, initiated by a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains the relative URIs of all
        direct transfers initiated by a given creditor, which have not
        been deleted yet. The returned fragment will not be sorted in
        any particular order.

        """

        try:
            transfer_uuids = procedures.get_creditor_transfer_uuids(creditorId)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        return transfer_uuids

    @transfers_api.arguments(DirectTransferCreationRequestSchema)
    @transfers_api.response(DirectTransferSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @transfers_api.doc(responses={303: specs.TRANSFER_EXISTS,
                                  403: specs.TOO_MANY_TRANSFERS,
                                  404: specs.CREDITOR_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_CONFLICT})
    def post(self, transfer_creation_request, creditorId):
        """Create a new direct transfer."""

        uuid = transfer_creation_request['transfer_uuid']
        recipient_account_uri = transfer_creation_request['recipient_account_uri']
        location = url_for('transfers.TransferEndpoint', _external=True, creditorId=creditorId, transferUuid=uuid)
        try:
            recipient_account_data = endpoints.match_url('account', recipient_account_uri)
        except endpoints.MatchError:
            recipient_account_data = {}
        try:
            transfer = procedures.initiate_transfer(
                creditor_id=creditorId,
                transfer_uuid=uuid,
                debtor_id=recipient_account_data.get('debtorId'),
                recipient_creditor_id=recipient_account_data.get('creditorId'),
                amount=transfer_creation_request['amount'],
                transfer_info=transfer_creation_request['info'],
            )
        except procedures.TooManyManagementActionsError:
            abort(403)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        except procedures.TransfersConflictError:
            abort(409)
        except procedures.TransferExistsError:
            return redirect(location, code=303)
        return transfer, {'Location': location}


@transfers_api.route('/<i64:creditorId>/transfers/<uuid:transferUuid>', parameters=[CID, TRANSFER_UUID])
class DirectTransferEndpoint(MethodView):
    @transfers_api.response(DirectTransferSchema(context=CONTEXT))
    @transfers_api.doc(responses={404: specs.TRANSFER_DOES_NOT_EXIST})
    def get(self, creditorId, transferUuid):
        """Return information about a direct transfer."""

        return procedures.get_initiated_transfer(creditorId, transferUuid) or abort(404)

    @transfers_api.arguments(DirectTransferUpdateRequestSchema)
    @transfers_api.response(DirectTransferSchema(context=CONTEXT))
    @transfers_api.doc(responses={404: specs.TRANSFER_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_UPDATE_CONFLICT})
    def patch(self, transfer_update_request, creditorId, transferUuid):
        """Cancel a direct transfer, if possible.

        This operation is **idempotent**!

        """

        try:
            if not (transfer_update_request['is_finalized'] and not transfer_update_request['is_successful']):
                raise procedures.TransferUpdateConflictError()
            transfer = procedures.cancel_transfer(creditorId, transferUuid)
        except procedures.TransferDoesNotExistError:
            abort(404)
        except procedures.TransferUpdateConflictError:
            abort(409)
        return transfer

    @transfers_api.response(code=204)
    def delete(self, creditorId, transferUuid):
        """Delete a direct transfer.

        Note that deleting a running (not finalized) transfer does not
        cancel it. To ensure that a running transfer has not been
        successful, it must be canceled before deletion.

        """

        procedures.delete_initiated_transfer(creditorId, transferUuid)
