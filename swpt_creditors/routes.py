from typing import NamedTuple
from urllib.parse import urlencode
from flask import redirect, url_for
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import missing
from swpt_lib import endpoints
from .schemas import (
    CreditorCreationRequestSchema, CreditorSchema, DebtorSchema, AccountListSchema, TransferListSchema,
    AccountSchema, AccountConfigSchema, CommittedTransferSchema, LedgerEntriesPageSchema,
    WalletSchema, ObjectReferencesPageSchema, PaginationParametersSchema, LogEntriesPageSchema,
    TransferCreationRequestSchema, TransferSchema, CancelTransferRequestSchema,
    AccountDisplaySchema, AccountExchangeSchema, AccountIdentitySchema, AccountKnowledgeSchema,
    AccountLedgerSchema, AccountInfoSchema, ObjectReferenceSchema,
)
from .specs import DID, CID, EPOCH, SEQNUM, TRANSFER_UUID
from . import specs
from . import procedures


class PaginatedList(NamedTuple):
    itemsType: str
    first: str
    forthcoming: str = missing
    creditorId: int = None

    @property
    def wallet(self):
        if self.creditorId is None:
            return missing
        return {'uri': url_for('creditors.WalletEndpoint', creditorId=self.creditorId)}


CONTEXT = {
    'Creditor': 'creditors.CreditorEndpoint',
    'Wallet': 'creditors.WalletEndpoint',
    'AccountList': 'creditors.AccountListEndpoint',
    'TransferList': 'creditors.TransferListEndpoint',
    'Account': 'accounts.AccountEndpoint',
    'AccountDisplay': 'accounts.AccountDisplayEndpoint',
    'AccountExchange': 'accounts.AccountExchangeEndpoint',
    'AccountKnowledge': 'accounts.AccountKnowledgeEndpoint',
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

    @creditors_api.arguments(CreditorCreationRequestSchema)
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


@creditors_api.route('/<i64:creditorId>/wallet', parameters=[CID])
class WalletEndpoint(MethodView):
    @creditors_api.response(WalletSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getWallet',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return creditor's wallet.

        The creditor's wallet "contains" all creditor's accounts,
        pending transfers, and recent events (the log). In short: it
        is the gateway to all objects and operations in the API.

        """

        wallet = procedures.get_creditor(creditorId)
        if not wallet:
            abort(404)

        log_url = url_for('.LogEntriesEndpoint', creditorId=creditorId)
        log_q = urlencode({'prev': wallet.latest_log_entry_id})
        wallet.log = PaginatedList('LogEntry', log_url, forthcoming=f'{log_url}?{log_q}')
        wallet.transferList = {'uri': url_for('creditors.TransferListEndpoint', creditorId=creditorId)}
        wallet.accountList = {'uri': url_for('creditors.AccountListEndpoint', creditorId=creditorId)}
        wallet.creditor = {'uri': url_for('creditors.CreditorEndpoint', creditorId=creditorId)}
        return wallet


@creditors_api.route('/<i64:creditorId>/log', parameters=[CID])
class LogEntriesEndpoint(MethodView):
    @creditors_api.arguments(PaginationParametersSchema, location='query')
    @creditors_api.response(LogEntriesPageSchema(context=CONTEXT), example=specs.LOG_ENTRIES_EXAMPLE)
    @creditors_api.doc(operationId='getLogPage',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of creditor's recent log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recent log entries. The
        returned fragment will be sorted in chronological order
        (smaller `entryId`s go first).

        """

        abort(500)


@creditors_api.route('/<i64:creditorId>/account-list', parameters=[CID])
class AccountListEndpoint(MethodView):
    @creditors_api.response(AccountListSchema(context=CONTEXT), example=specs.ACCOUNT_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getAccountList',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a paginated list of links to all accounts belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        abort(500)


@creditors_api.route('/<i64:creditorId>/transfer-list', parameters=[CID])
class TransferListEndpoint(MethodView):
    @creditors_api.response(TransferListSchema(context=CONTEXT), example=specs.TRANSFER_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getTransferList',
                       responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a paginated list of links to all transfers belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        abort(500)


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="Create, view, update, and delete accounts, view account's transaction history.",
)


@accounts_api.route('/<i64:creditorId>/debtor-lookup', parameters=[CID])
class DebtorLookupEndpoint(MethodView):
    @accounts_api.arguments(DebtorSchema, example=specs.DEBTOR_EXAMPLE)
    @accounts_api.response(ObjectReferenceSchema(context=CONTEXT), example=specs.ACCOUNT_LOOKUP_RESPONSE_EXAMPLE)
    @accounts_api.doc(operationId='debtorLookup',
                      responses={204: specs.NO_ACCOUNT_WITH_THIS_DEBTOR,
                                 404: specs.CREDITOR_DOES_NOT_EXIST,
                                 422: specs.UNRECOGNIZED_DEBTOR})
    def post(self, account_info, creditorId):
        """Try to find an existing account with a given debtor.

        This is useful when the creditor wants not know if he already
        has an account with a given debtor, and if not, whether the
        debtor's URI is recognized by the system.

        """

        abort(422, errors={"uri": ["The debtor's URI can not be recognized."]})


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[CID])
class AccountsEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(ObjectReferencesPageSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountsPage',
                      responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of accounts belonging to a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all `Account`s
        belonging to a given creditor. The returned fragment will not
        be sorted in any particular order.

        """

        try:
            debtor_ids = procedures.get_account_dedtor_ids(creditorId)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        return debtor_ids

    @accounts_api.arguments(DebtorSchema, example=specs.DEBTOR_EXAMPLE)
    @accounts_api.response(AccountSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @accounts_api.doc(operationId='createAccount',
                      responses={303: specs.ACCOUNT_EXISTS,
                                 403: specs.DENIED_ACCOUNT_CREATION,
                                 404: specs.CREDITOR_DOES_NOT_EXIST,
                                 422: specs.UNRECOGNIZED_DEBTOR})
    def post(self, debtor, creditorId):
        """Create a new account belonging to a given creditor."""

        debtor_uri = debtor['uri']
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
            abort(422, errors={"uri": ["The debtor's URI can not be recognized."]})
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
    @accounts_api.doc(operationId='deleteAccount',
                      responses={403: specs.UNSAFE_ACCOUNT_DELETION,
                                 409: specs.PEG_ACCOUNT_DELETION})
    def delete(self, creditorId, debtorId):
        """Delete an account.

        **Important note:** This operation will succeed only if the
        account is marked as safe for deletion, or unsafe deletion is
        allowed for the account.

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
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's configuration."""

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/display', parameters=[CID, DID])
class AccountDisplayEndpoint(MethodView):
    @accounts_api.response(AccountDisplaySchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountDisplay',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's display settings."""

        abort(500)

    @accounts_api.arguments(AccountDisplaySchema)
    @accounts_api.response(AccountDisplaySchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountDisplay',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_DISPLAY_UPDATE_CONFLICT,
                                 422: specs.UNRECOGNIZED_PEG_CURRENCY})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's display settings."""

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/exchange', parameters=[CID, DID])
class AccountExchangeEndpoint(MethodView):
    @accounts_api.response(AccountExchangeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountExchange',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's exchange settings."""

        abort(500)

    @accounts_api.arguments(AccountExchangeSchema)
    @accounts_api.response(AccountExchangeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountExchange',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST,
                                 422: specs.INVALID_EXCHANGE_POLICY})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's exchange settings."""

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/knowledge', parameters=[CID, DID])
class AccountKnowledgeEndpoint(MethodView):
    @accounts_api.response(AccountKnowledgeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountKnowledge',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return the acknowledged account information.

        The returned object contains information that has been made
        known to the creditor (the owner of the account). This is
        useful, for example, to decide whether the creditor has been
        informed already about an important change in the account's
        status that has occurred.

        """

        abort(500)

    @accounts_api.arguments(AccountKnowledgeSchema)
    @accounts_api.response(AccountKnowledgeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountKnowledge',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def patch(self, knowledge_update_request, creditorId, debtorId):
        """Update the acknowledged account information.

        This operation should be performed when an important change in
        the account's status, that has occurred, has been made known
        to the creditor (the owner of the account).

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/info', parameters=[CID, DID])
class AccountInfoEndpoint(MethodView):
    @accounts_api.response(AccountInfoSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountInfo',
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
        """Return account's ledger."""

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[CID, DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LedgerEntriesPageSchema(context=CONTEXT), example=specs.ACCOUNT_LEDGER_ENTRIES_EXAMPLE)
    @accounts_api.doc(operationId='getAccountLedgerEntriesPage',
                      responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId, debtorId):
        """Return a collection of ledger entries for a given account.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains the ledger entries for a
        given account. The returned fragment, and all the subsequent
        fragments, will be sorted in reverse-chronological order
        (bigger `entryId`s go first). The entries will constitute a
        singly linked list, each entry (except the most ancient one)
        referring to its ancestor. Note that:

        * If the `prev` URL query parameter is not specified, then the
          returned fragment will start with the latest ledger entry
          for the given account.

        * If the `prev` URL query parameter is specified, then the
          returned fragment will start with the latest ledger entry
          for the given account, which have smaller `entryId` than the
          specified value.

        * When the `stop` URL query parameter contains the `entryId`
          of a ledger entry, then the returned fragment, and all the
          subsequent fragments, will contain only ledger entries that
          are newer than that entry (have bigger entry IDs than the
          specified one). This can be used to prevent repeatedly
          receiving ledger entries that the client already knows
          about.

        """

        abort(500)


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/creditors',
    description="Make transfers from one account to another account.",
)


@transfers_api.route('/<i64:creditorId>/account-lookup', parameters=[CID])
class AccountLookupEndpoint(MethodView):
    @transfers_api.arguments(AccountIdentitySchema, example=specs.ACCOUNT_LOOKUP_REQUEST_EXAMPLE)
    @transfers_api.response(ObjectReferenceSchema(context=CONTEXT), example=specs.ACCOUNT_LOOKUP_RESPONSE_EXAMPLE)
    @transfers_api.doc(operationId='accountLookup',
                       responses={204: specs.NO_MATCHING_ACCOUNT,
                                  404: specs.CREDITOR_DOES_NOT_EXIST})
    def post(self, account_info, creditorId):
        """Given recipient's account identity, try to find a matching sender
        account.

        This is useful when the creditor wants to send money to some
        other creditor's account, but he does not know if he already
        has an account with the same debtor (that is: the debtor of
        the other creditor's account).

        """

        abort(500)


@transfers_api.route('/<i64:creditorId>/transfers/', parameters=[CID])
class TransfersEndpoint(MethodView):
    @transfers_api.arguments(PaginationParametersSchema, location='query')
    @transfers_api.response(ObjectReferencesPageSchema(context=CONTEXT), example=specs.TRANSFER_LINKS_EXAMPLE)
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
                                  403: specs.DENIED_TRANSFER,
                                  404: specs.CREDITOR_DOES_NOT_EXIST,
                                  409: specs.TRANSFER_CONFLICT,
                                  422: specs.INVALID_TRANSFER_CREATION_REQUEST})
    def post(self, transfer_creation_request, creditorId):
        """Initiate a transfer."""

        uuid = transfer_creation_request['transfer_uuid']
        location = url_for('transfers.TransferEndpoint', _external=True, creditorId=creditorId, transferUuid=uuid)
        try:
            # TODO: parse `transfer_creation_request['recipient']`.
            debtor_id, recipient = 1, 'xxx'
        except ValueError:
            abort(422, errors={"recipient": {"uri": ["The recipient's URI can not be recognized."]}})
        try:
            transfer = procedures.initiate_transfer(
                creditor_id=creditorId,
                transfer_uuid=uuid,
                debtor_id=debtor_id,
                recipient=recipient,
                amount=transfer_creation_request['amount'],
                transfer_note=transfer_creation_request['note'],
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
        """Return a transfer."""

        return procedures.get_direct_transfer(creditorId, transferUuid) or abort(404)

    @transfers_api.arguments(CancelTransferRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='cancelTransfer',
                       responses={403: specs.TRANSFER_CANCELLATION_FAILURE,
                                  404: specs.TRANSFER_DOES_NOT_EXIST})
    def post(self, cancel_transfer_request, creditorId, transferUuid):
        """Cancel a transfer.

        This operation will fail if the transfer can not be canceled.

        """

        try:
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


@transfers_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<int:epoch>/<i64:seqnum>',
                     parameters=[CID, DID, EPOCH, SEQNUM])
class CommittedTransferEndpoint(MethodView):
    @transfers_api.response(CommittedTransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='getCommittedTransfer',
                       responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId, epoch, seqnum):
        """Return information about sent or received transfer."""

        abort(500)
