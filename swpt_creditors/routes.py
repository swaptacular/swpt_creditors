from functools import partial
from datetime import date, timedelta
from flask import current_app, redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib.utils import i64_to_u64
from swpt_lib.swpt_uris import parse_debtor_uri, parse_account_uri, make_debtor_uri
from swpt_creditors.models import MAX_INT64, DATE0
from swpt_creditors.schemas import (
    CreditorCreationRequestSchema, CreditorSchema, DebtorIdentitySchema, TransferListSchema,
    AccountSchema, AccountConfigSchema, CommittedTransferSchema, LedgerEntriesPageSchema,
    WalletSchema, ObjectReferencesPageSchema, PaginationParametersSchema, LogEntriesPageSchema,
    TransferCreationRequestSchema, TransferSchema, CancelTransferRequestSchema,
    AccountDisplaySchema, AccountExchangeSchema, AccountIdentitySchema, AccountKnowledgeSchema,
    AccountLedgerSchema, AccountInfoSchema, AccountListSchema, LogPaginationParamsSchema,
    AccountsPaginationParamsSchema,
)
from swpt_creditors.specs import DID, CID, TID, TRANSFER_UUID
from swpt_creditors import specs
from swpt_creditors import procedures


def _url_for(name):
    return staticmethod(partial(url_for, name, _external=False))


class path_builder:
    creditor = _url_for('creditors.CreditorEndpoint')
    wallet = _url_for('creditors.WalletEndpoint')
    log_entries = _url_for('creditors.LogEntriesEndpoint')
    account_list = _url_for('creditors.AccountListEndpoint')
    transfer_list = _url_for('creditors.TransferListEndpoint')
    account = _url_for('accounts.AccountEndpoint')
    account_info = _url_for('accounts.AccountInfoEndpoint')
    account_ledger = _url_for('accounts.AccountLedgerEndpoint')
    account_display = _url_for('accounts.AccountDisplayEndpoint')
    account_exchange = _url_for('accounts.AccountExchangeEndpoint')
    account_knowledge = _url_for('accounts.AccountKnowledgeEndpoint')
    account_config = _url_for('accounts.AccountConfigEndpoint')
    account_ledger_entries = _url_for('accounts.AccountLedgerEntriesEndpoint')
    accounts = _url_for('accounts.AccountsEndpoint')
    account_lookup = _url_for('accounts.AccountLookupEndpoint')
    debtor_lookup = _url_for('accounts.DebtorLookupEndpoint')
    transfer = _url_for('transfers.TransferEndpoint')
    transfers = _url_for('transfers.TransfersEndpoint')
    committed_transfer = _url_for('transfers.CommittedTransferEndpoint')


class schema_types:
    creditor = 'Creditor'
    account = 'Account'


CONTEXT = {'paths': path_builder}
procedures.init(path_builder, schema_types)


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Get information about creditors, create new creditors.",
)


@creditors_api.route('/<i64:creditorId>/', parameters=[CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(CreditorSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getCreditor')
    def get(self, creditorId):
        """Return a creditor."""

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor

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
        return creditor, {'Location': path_builder.creditor(creditorId=creditorId)}

    @creditors_api.arguments(CreditorSchema)
    @creditors_api.response(CreditorSchema(context=CONTEXT))
    @creditors_api.doc(operationId='updateCreditor')
    def patch(self, creditor, creditorId):
        """Update a creditor.

        **Note:** Currently there are no fields that can be updated,
        but they may be added in the future.

        """

        try:
            creditor = procedures.update_creditor(creditorId)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        return creditor


@creditors_api.route('/<i64:creditorId>/wallet', parameters=[CID])
class WalletEndpoint(MethodView):
    @creditors_api.response(WalletSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getWallet')
    def get(self, creditorId):
        """Return creditor's wallet.

        The creditor's wallet "contains" all creditor's accounts,
        pending transfers, and recent events (the log). In short: it
        is the gateway to all objects and operations in the API.

        """

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor


@creditors_api.route('/<i64:creditorId>/log', parameters=[CID])
class LogEntriesEndpoint(MethodView):
    @creditors_api.arguments(LogPaginationParamsSchema, location='query')
    @creditors_api.response(LogEntriesPageSchema(context=CONTEXT), example=specs.LOG_ENTRIES_EXAMPLE)
    @creditors_api.doc(operationId='getLogPage')
    def get(self, pagination_params, creditorId):
        """Return a collection of creditor's recent log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recent log entries. The
        returned fragment will be sorted in chronological order
        (smaller `entryId`s go first). The log entries will constitute
        a singly linked list, each entry (except the most ancient one)
        referring to its ancestor.

        """

        n = current_app.config['APP_LOG_ENTRIES_PER_PAGE']
        try:
            creditor, entries = procedures.get_log_entries(creditorId, n, pagination_params['prev'])
        except procedures.CreditorDoesNotExistError:
            abort(404)

        if len(entries) < n:
            proceed = 'forthcoming'
            entry_id = creditor.latest_log_entry_id
        else:
            proceed = 'next'
            entry_id = entries[-1].entry_id

        return {
            'uri': request.full_path,
            'items': entries,
            proceed: f'?prev={entry_id}',
        }


@creditors_api.route('/<i64:creditorId>/account-list', parameters=[CID])
class AccountListEndpoint(MethodView):
    @creditors_api.response(AccountListSchema(context=CONTEXT), example=specs.ACCOUNT_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getAccountList')
    def get(self, creditorId):
        """Return a paginated list of links to all accounts belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor


@creditors_api.route('/<i64:creditorId>/transfer-list', parameters=[CID])
class TransferListEndpoint(MethodView):
    @creditors_api.response(TransferListSchema(context=CONTEXT), example=specs.TRANSFER_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getTransferList')
    def get(self, creditorId):
        """Return a paginated list of links to all transfers belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="Create, view, update, and delete accounts, view account's transaction history.",
)


@accounts_api.route('/<i64:creditorId>/account-lookup', parameters=[CID])
class AccountLookupEndpoint(MethodView):
    @accounts_api.arguments(AccountIdentitySchema, example=specs.ACCOUNT_IDENTITY_EXAMPLE)
    @accounts_api.response(DebtorIdentitySchema)
    @accounts_api.doc(operationId='accountLookup')
    def post(self, account_identity, creditorId):
        """Given an account identity, find the debtor's identity.

        This can be useful, for example, when the creditor wants to
        send money to some other creditor's account, but he does not
        know if he already has an account with the same debtor (that
        is: the debtor of the other creditor's account).

        """

        try:
            debtorId, _ = parse_account_uri(account_identity['uri'])
        except ValueError:
            abort(422, errors={'json': {'uri': ['The URI can not be recognized.']}})

        return {'uri': make_debtor_uri(debtorId)}


@accounts_api.route('/<i64:creditorId>/debtor-lookup', parameters=[CID])
class DebtorLookupEndpoint(MethodView):
    @accounts_api.arguments(DebtorIdentitySchema, example=specs.DEBTOR_IDENTITY_EXAMPLE)
    @accounts_api.response(code=204)
    @accounts_api.doc(operationId='debtorLookup',
                      responses={204: specs.NO_ACCOUNT_WITH_THIS_DEBTOR,
                                 303: specs.ACCOUNT_EXISTS})
    def post(self, debtor_identity, creditorId):
        """Try to find an existing account with a given debtor.

        This is useful when the creditor wants not know if he already
        has an account with a given debtor, and if not, whether the
        debtor's identity is recognized by the system.

        """

        try:
            debtorId = parse_debtor_uri(debtor_identity['uri'])
        except ValueError:
            abort(422, errors={'json': {'uri': ['The URI can not be recognized.']}})

        if procedures.get_account(creditorId, debtorId):
            location = url_for('accounts.AccountEndpoint', _external=True, creditorId=creditorId, debtorId=debtorId)
            return redirect(location, code=303)


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[CID])
class AccountsEndpoint(MethodView):
    @accounts_api.arguments(AccountsPaginationParamsSchema, location='query')
    @accounts_api.response(ObjectReferencesPageSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountsPage')
    def get(self, pagination_params, creditorId):
        """Return a collection of accounts belonging to a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all `Account`s
        belonging to a given creditor. The returned fragment will not
        be sorted in any particular order.

        """

        n = current_app.config['APP_ACCOUNTS_PER_PAGE']
        ids = procedures.get_account_debtor_ids(creditorId, n, pagination_params.get('prev'))
        page = {
            'uri': request.full_path,
            'items': [{'uri': f'{i64_to_u64(debtorId)}/'} for debtorId in ids],
        }
        if len(ids) >= n:
            page['next'] = f'?prev={ids[-1]}'

        return page

    @accounts_api.arguments(DebtorIdentitySchema, example=specs.DEBTOR_IDENTITY_EXAMPLE)
    @accounts_api.response(AccountSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @accounts_api.doc(operationId='createAccount',
                      responses={303: specs.ACCOUNT_EXISTS,
                                 403: specs.DENIED_ACCOUNT_CREATION})
    def post(self, debtor_identity, creditorId):
        """Create a new account belonging to a given creditor."""

        try:
            debtorId = parse_debtor_uri(debtor_identity['uri'])
        except ValueError:
            abort(422, errors={'json': {'uri': ['The URI can not be recognized.']}})

        location = url_for('accounts.AccountEndpoint', _external=True, creditorId=creditorId, debtorId=debtorId)
        try:
            account = procedures.create_new_account(creditorId, debtorId)
        except procedures.ForbiddenAccountCreationError:
            abort(403)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        except procedures.AccountExistsError:
            return redirect(location, code=303)

        return account, {'Location': location}


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/', parameters=[CID, DID])
class AccountEndpoint(MethodView):
    @accounts_api.response(AccountSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccount')
    def get(self, creditorId, debtorId):
        """Return an account."""

        account = procedures.get_account(creditorId, debtorId, join=True)
        if account is None:
            abort(404)
        return account

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
    @accounts_api.doc(operationId='getAccountConfig')
    def get(self, creditorId, debtorId):
        """Return account's configuration."""

        abort(404)

    @accounts_api.arguments(AccountConfigSchema)
    @accounts_api.response(AccountConfigSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountConfig')
    def patch(self, account_config, creditorId, debtorId):
        """Update account's configuration."""

        abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/display', parameters=[CID, DID])
class AccountDisplayEndpoint(MethodView):
    @accounts_api.response(AccountDisplaySchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountDisplay')
    def get(self, creditorId, debtorId):
        """Return account's display settings."""

        abort(404)

    @accounts_api.arguments(AccountDisplaySchema)
    @accounts_api.response(AccountDisplaySchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountDisplay',
                      responses={409: specs.ACCOUNT_DISPLAY_UPDATE_CONFLICT})
    def patch(self, account_display, creditorId, debtorId):
        """Update account's display settings."""

        # TODO: Should return 422 if the peg currency is not recognized.

        abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/exchange', parameters=[CID, DID])
class AccountExchangeEndpoint(MethodView):
    @accounts_api.response(AccountExchangeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountExchange')
    def get(self, creditorId, debtorId):
        """Return account's exchange settings."""

        abort(404)

    @accounts_api.arguments(AccountExchangeSchema)
    @accounts_api.response(AccountExchangeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountExchange')
    def patch(self, account_exchange, creditorId, debtorId):
        """Update account's exchange settings."""

        # TODO: Should return 422 if the exchange policy is not recognized.

        abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/knowledge', parameters=[CID, DID])
class AccountKnowledgeEndpoint(MethodView):
    @accounts_api.response(AccountKnowledgeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountKnowledge')
    def get(self, creditorId, debtorId):
        """Return the acknowledged account information.

        The returned object contains information that has been made
        known to the creditor (the owner of the account). This is
        useful, for example, to decide whether the creditor has been
        informed already about an important change in the account's
        status that has occurred.

        """

        abort(404)

    @accounts_api.arguments(AccountKnowledgeSchema)
    @accounts_api.response(AccountKnowledgeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountKnowledge')
    def patch(self, account_knowledge, creditorId, debtorId):
        """Update the acknowledged account information.

        This operation should be performed when an important change in
        the account's status, that has occurred, has been made known
        to the creditor (the owner of the account).

        """

        abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/info', parameters=[CID, DID])
class AccountInfoEndpoint(MethodView):
    @accounts_api.response(AccountInfoSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountInfo')
    def get(self, creditorId, debtorId):
        """Return account's status information."""

        abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/ledger', parameters=[CID, DID])
class AccountLedgerEndpoint(MethodView):
    @accounts_api.response(AccountLedgerSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountLedger')
    def get(self, creditorId, debtorId):
        """Return account's ledger."""

        abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[CID, DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LedgerEntriesPageSchema(context=CONTEXT), example=specs.ACCOUNT_LEDGER_ENTRIES_EXAMPLE)
    @accounts_api.doc(operationId='getAccountLedgerEntriesPage')
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

        abort(404)


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/creditors',
    description="Make transfers from one account to another account.",
)


@transfers_api.route('/<i64:creditorId>/transfers/', parameters=[CID])
class TransfersEndpoint(MethodView):
    @transfers_api.arguments(PaginationParametersSchema, location='query')
    @transfers_api.response(ObjectReferencesPageSchema(context=CONTEXT), example=specs.TRANSFER_LINKS_EXAMPLE)
    @transfers_api.doc(operationId='getTransfersPage')
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
                                  409: specs.TRANSFER_CONFLICT})
    def post(self, transfer_creation_request, creditorId):
        """Initiate a transfer."""

        uuid = transfer_creation_request['transfer_uuid']
        location = url_for('transfers.TransferEndpoint', _external=True, creditorId=creditorId, transferUuid=uuid)
        try:
            # TODO: parse `transfer_creation_request['recipient']`.
            debtor_id, recipient = 1, 'xxx'
        except ValueError:
            abort(422, errors={'json': {'recipient': {'uri': ["The recipient's URI can not be recognized."]}}})
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
    @transfers_api.doc(operationId='getTransfer')
    def get(self, creditorId, transferUuid):
        """Return a transfer."""

        return procedures.get_direct_transfer(creditorId, transferUuid) or abort(404)

    @transfers_api.arguments(CancelTransferRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='cancelTransfer',
                       responses={403: specs.TRANSFER_CANCELLATION_FAILURE})
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


@transfers_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<transferId>', parameters=[CID, DID, TID])
class CommittedTransferEndpoint(MethodView):
    @transfers_api.response(CommittedTransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='getCommittedTransfer')
    def get(self, creditorId, debtorId, transferId):
        """Return information about sent or received transfer."""

        try:
            epoch, n = transferId.split('-', maxsplit=1)
            creation_date = DATE0 + timedelta(days=int(epoch))
            transfer_number = int(n)
            if not 1 <= transfer_number <= MAX_INT64:
                raise ValueError
        except (ValueError, OverflowError):
            abort(404)

        assert isinstance(creation_date, date)
        assert isinstance(transfer_number, int)
        abort(500)
