from typing import NamedTuple
from urllib.parse import urlencode
from flask import redirect, url_for
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import missing
from swpt_lib import endpoints
from .schemas import (
    CreditorCreationOptionsSchema, CreditorSchema, AccountCreationRequestSchema,
    AccountSchema, AccountConfigSchema, CommittedTransferSchema, LedgerEntriesPage,
    PortfolioSchema, ObjectReferencesPage, PaginationParametersSchema, LogEntriesPageSchema,
    TransferCreationRequestSchema, TransferSchema, TransferUpdateRequestSchema,
    AccountDisplaySettingsSchema, AccountExchangeSettingsSchema, AccountInfoSchema,
    AccountLedgerSchema, AccountStatusSchema,
)
from .specs import DID, CID, SEQNUM, TRANSFER_UUID
from . import specs
from . import procedures


class PaginatedList(NamedTuple):
    itemsType: str
    first: str
    forthcoming: str = missing
    totalItems: int = missing


CONTEXT = {
    'Creditor': 'creditors.CreditorEndpoint',
    'Portfolio': 'creditors.PortfolioEndpoint',
    'AccountList': 'accounts.AccountListEndpoint',
    'Account': 'accounts.AccountEndpoint',
    'Accounts': 'accounts.AccountsEndpoint',
    'Transfer': 'transfers.TransferEndpoint',
    'Transfers': 'transfers.TransfersEndpoint',
}


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Get information about creditors, create new creditors.",
)


@creditors_api.route('/<i64:creditorId>/', parameters=[CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(CreditorSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getCreditor',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return a creditor."""

        creditor = procedures.get_creditor(creditorId)
        if not creditor:
            abort(404)
        return creditor, {'Cache-Control': 'max-age=86400'}

    @creditors_api.arguments(CreditorCreationOptionsSchema)
    @creditors_api.response(CreditorSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @creditors_api.doc(operationId='createCreditor',
                       responses={409: specs.CONFLICTING_CREDITOR})
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


@creditors_api.route('/<i64:creditorId>/portfolio', parameters=[CID])
class PortfolioEndpoint(MethodView):
    @creditors_api.response(PortfolioSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getPortfolio',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return creditor's portfolio."""

        portfolio = procedures.get_creditor(creditorId)
        if not portfolio:
            abort(404)

        log_url = url_for('.LogEntriesEndpoint', creditorId=creditorId)
        log_q = urlencode({'prev': portfolio.latest_log_entry_id})
        portfolio.log = PaginatedList('LogEntry', log_url, forthcoming=f'{log_url}?{log_q}')

        transfers_url = url_for('transfers.TransfersEndpoint', creditorId=creditorId)
        transfers_count = portfolio.direct_transfers_count
        portfolio.transfers = PaginatedList('string', transfers_url, totalItems=transfers_count)

        accounts_url = url_for('accounts.AccountsEndpoint', creditorId=creditorId)
        accounts_count = portfolio.accounts_count
        portfolio.accounts = PaginatedList('string', accounts_url, totalItems=accounts_count)

        portfolio.creditor = {'uri': url_for('creditors.CreditorEndpoint', creditorId=portfolio.creditor_id)}
        return portfolio


@creditors_api.route('/<i64:creditorId>/log', parameters=[CID])
class LogEntriesEndpoint(MethodView):
    @creditors_api.arguments(PaginationParametersSchema, location='query')
    @creditors_api.response(LogEntriesPageSchema(context=CONTEXT), example=specs.LOG_ENTRIES_EXAMPLE)
    @creditors_api.doc(operationId='getLogPage',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of creditor's recently posted log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recently posted log
        entries. The returned fragment will be sorted in chronological
        order (smaller entry IDs go first).

        """

        abort(500)


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="Create, view, update, and delete accounts, view account's transaction history.",
)


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[CID])
class AccountsEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(ObjectReferencesPage(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountsPage',
                      responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of accounts belonging to a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all accounts
        belonging to a given creditor. The returned fragment will not
        be sorted in any particular order.

        """

        try:
            debtor_ids = procedures.get_account_dedtor_ids(creditorId)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        return debtor_ids

    @accounts_api.arguments(AccountCreationRequestSchema)
    @accounts_api.response(AccountSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @accounts_api.doc(operationId='createAccount',
                      responses={303: specs.ACCOUNT_EXISTS,
                                 403: specs.TOO_MANY_ACCOUNTS,
                                 404: specs.CREDITOR_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_CONFLICT,
                                 422: specs.ACCOUNT_CAN_NOT_BE_CREATED})
    def post(self, account_creation_request, creditorId):
        """Create a new account belonging to a given creditor."""

        debtor_uri = account_creation_request['debtor_uri']
        try:
            debtor_id = endpoints.match_url('debtor', debtor_uri)['debtorId']
            location = url_for(
                'accounts.AccountEndpoint',
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
class AccountEndpoint(MethodView):
    @accounts_api.response(AccountSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccount',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return an account."""

        abort(500)

    @accounts_api.response(code=204)
    @accounts_api.doc(operationId='deleteAccount')
    def delete(self, creditorId, debtorId):
        """Delete an account.

        **Important note:** If the account is not marked as safe for
        deletion, deleting it may result in losing a non-negligible
        amount of money on the account.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/config', parameters=[CID, DID])
class AccountConfigEndpoint(MethodView):
    @accounts_api.response(AccountConfigSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountConfig',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's configuration."""

        abort(500)

    @accounts_api.arguments(AccountConfigSchema)
    @accounts_api.response(AccountConfigSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountConfig',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_UPDATE_CONFLICT})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's configuration.

        **Note:** This operation is idempotent.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/display', parameters=[CID, DID])
class AccountDisplaySettingsEndpoint(MethodView):
    @accounts_api.response(AccountDisplaySettingsSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountDisplaySettings',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's display settings."""

        abort(500)

    @accounts_api.arguments(AccountDisplaySettingsSchema)
    @accounts_api.response(AccountDisplaySettingsSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountDisplaySettings',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_UPDATE_CONFLICT})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's display settings.

        **Note:** This operation is idempotent.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/exchange', parameters=[CID, DID])
class AccountExchangeSettingsEndpoint(MethodView):
    @accounts_api.response(AccountExchangeSettingsSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountExchangeSettings',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's exchange settings."""

        abort(500)

    @accounts_api.arguments(AccountExchangeSettingsSchema)
    @accounts_api.response(AccountExchangeSettingsSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountExchangeSettings',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_UPDATE_CONFLICT})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's exchange settings.

        **Note:** This operation is idempotent.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/status', parameters=[CID, DID])
class AccountStatusEndpoint(MethodView):
    @accounts_api.response(AccountStatusSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountStatus',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's status information."""

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/ledger', parameters=[CID, DID])
class AccountLedgerEndpoint(MethodView):
    @accounts_api.response(AccountLedgerSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountLedger',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's ledger information."""

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[CID, DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LedgerEntriesPage(context=CONTEXT), example=specs.ACCOUNT_LEDGER_ENTRIES_EXAMPLE)
    @accounts_api.doc(operationId='getAccountLedgerEntriesPage',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId, debtorId):
        """Return a collection of ledger entries for a given account.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recent ledger entries
        for a given account. The returned fragment will be sorted in
        reverse-chronological order (bigger entry IDs go first). The
        entries will constitute a singly linked list, each entry
        (except the most ancient one) referring to its ancestor.

        """

        abort(500)


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/creditors',
    description="Make transfers from one account to another account.",
)


@transfers_api.route('/<i64:creditorId>/find-account', parameters=[CID])
class FindAccountEndpoint(MethodView):
    @transfers_api.arguments(AccountInfoSchema, example=specs.FIND_ACCOUNT_REQUEST_EXAMPLE)
    @transfers_api.response(AccountSchema(context=CONTEXT))
    @transfers_api.doc(operationId='findAccount',
                       responses={204: specs.NO_MATCHING_ACCOUNT,
                                  404: specs.CREDITOR_DOES_NOT_EXIST})
    def post(self, account_info, creditorId):
        """Given recipient's account information, try to find a matching
        sender account.

        This is useful when a creditor wants to send money to some
        other creditor's account, but he does not know if he already
        has an account with the same debtor (that is: the debtor of
        the other creditor's account).

        """

        abort(500)


@transfers_api.route('/<i64:creditorId>/transfers/', parameters=[CID])
class TransfersEndpoint(MethodView):
    @transfers_api.arguments(PaginationParametersSchema, location='query')
    @transfers_api.response(ObjectReferencesPage(context=CONTEXT), example=specs.TRANSFER_LINKS_EXAMPLE)
    @transfers_api.doc(operationId='getTransfersPage',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of transfers, initiated by a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all transfers
        initiated by a given creditor, which have not been deleted
        yet. The returned fragment will not be sorted in any
        particular order.

        """

        try:
            transfer_uuids = procedures.get_creditor_transfer_uuids(creditorId)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        return transfer_uuids

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @transfers_api.doc(operationId='createTransfer',
                       responses={303: specs.TRANSFER_EXISTS,
                                  403: specs.TOO_MANY_TRANSFERS,
                                  404: specs.CREDITOR_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_CONFLICT,
                                  422: specs.INVALID_TRANSFER_CREATION_REQUEST})
    def post(self, transfer_creation_request, creditorId):
        """Create a new transfer."""

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
                transfer_notes=transfer_creation_request['notes'],
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
class TransferEndpoint(MethodView):
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='getTransfer',
                       responses={404: specs.TRANSFER_DOES_NOT_EXIST})
    def get(self, creditorId, transferUuid):
        """Return information about a transfer."""

        return procedures.get_direct_transfer(creditorId, transferUuid) or abort(404)

    @transfers_api.arguments(TransferUpdateRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='cancelTransfer',
                       responses={404: specs.TRANSFER_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_UPDATE_CONFLICT})
    def patch(self, transfer_update_request, creditorId, transferUuid):
        """Cancel a transfer, if possible.

        **Note:** This operation is idempotent.

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
    @transfers_api.doc(operationId='deleteTransfer')
    def delete(self, creditorId, transferUuid):
        """Delete a transfer.

        Note that deleting a running (not finalized) transfer does not
        cancel it. To ensure that a running transfer has not been
        successful, it must be canceled before deletion.

        """

        procedures.delete_direct_transfer(creditorId, transferUuid)


@transfers_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<i64:seqnum>', parameters=[CID, DID, SEQNUM])
class CommittedTransferEndpoint(MethodView):
    @transfers_api.response(CommittedTransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='getCommittedTransfer',
                       responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId, seqnum):
        """Return information about sent or received transfer."""

        abort(500)
