from urllib.parse import urljoin
from flask import redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints
from .schemas import (
    CreditorCreationOptionsSchema, CreditorSchema, AccountCreationRequestSchema,
    AccountSchema, AccountRecordSchema, AccountRecordConfigSchema, CommittedTransferSchema,
    LedgerEntriesPage, PortfolioSchema, LinksPage, PaginationParametersSchema,
)
from . import specs
from . import procedures

CONTEXT = {
    'Creditor': 'creditors.CreditorEndpoint',
    'Account': 'creditors.AccountEndpoint',
    'TransfersCollection': 'transfers.TransfersCollectionEndpoint',
    'Transfer': 'transfers.TransferEndpoint',
    'AccountList': 'accounts.AccountListEndpoint',
    'AccountRecord': 'accounts.AccountRecordEndpoint',
}


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Obtain public information about accounts and creditors, create new creditors.",
)


@creditors_api.route('/<i64:creditorId>/', parameters=[specs.CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(CreditorSchema(context=CONTEXT))
    @creditors_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return public information about a creditor."""

        creditor = procedures.get_creditor(creditorId)
        if not creditor:
            abort(404)
        return creditor, {'Cache-Control': 'max-age=86400'}

    @creditors_api.arguments(CreditorCreationOptionsSchema)
    @creditors_api.response(CreditorSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @creditors_api.doc(responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_creation_options, creditorId):
        """Try to create a new creditor. Requires special privileges

        ---
        Must fail if the creditor already exists.

        """

        try:
            creditor = procedures.create_new_creditor(creditorId)
        except procedures.CreditorExistsError:
            abort(409)
        return creditor, {'Location': endpoints.build_url('creditor', creditorId=creditorId)}


@creditors_api.route('/<i64:creditorId>/portfolio', parameters=[specs.CID])
class PortfolioEndpoint(MethodView):
    @creditors_api.response(PortfolioSchema(context=CONTEXT))
    @creditors_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return creditor's portfolio."""

        abort(500)


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="View, create, update and delete creditors' accounts.",
)


@accounts_api.route('/<i64:creditorId>/debtors/<i64:debtorId>', parameters=[specs.CID, specs.DID])
class AccountEndpoint(MethodView):
    @accounts_api.response(AccountSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return public information about an account."""

        account = None
        if not account:
            abort(404)
        return account, {'Cache-Control': 'max-age=86400'}


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[specs.CID])
class AccountRecordsEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LinksPage(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId):
        """Return a collection of account record URIs.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains the relative URIs of all
        account records belonging to a given creditor.

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
        """Create a new account record."""

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


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/', parameters=[specs.CID, specs.DID])
class AccountRecordEndpoint(MethodView):
    @accounts_api.response(AccountRecordSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_RECORD_DOES_NOT_EXIST})
    def get(self, debtorId, creditorId):
        """Return an account record."""

        abort(500)

    @accounts_api.response(code=204)
    def delete(self, debtorId, transferUuid):
        """Delete an account record.

        **Important note:** If the account record is not marked as
        safe for deletion, deleting it may result in losing a
        non-negligible amount of money on the account.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/config', parameters=[specs.CID, specs.DID])
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

        **Note:** This operation is idempotent.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[specs.CID, specs.DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(PaginationParametersSchema, location='query')
    @accounts_api.response(LedgerEntriesPage(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_RECORD_DOES_NOT_EXIST})
    def get(self, pagination_parameters, creditorId, debtorId):
        """Return a collection of  account ledger entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains all recent ledger entries
        for a given account record. The returned fragment will be
        sorted in reverse-chronological order (bigger entry IDs go
        first). The entries will constitute a singly linked list, each
        entry (except the most ancient one) referring to its ancestor.

        """

        abort(500)


utils_api = Blueprint(
    'utils',
    __name__,
    url_prefix='/creditors',
    description="Miscellaneous utilities.",
)


@utils_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<i64:transferSeqnum>',
                 parameters=[specs.CID, specs.DID])
class AccountTransferEndpoint(MethodView):
    @utils_api.response(CommittedTransferSchema(context=CONTEXT))
    @utils_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId, transferSeqnum):
        """Return information about a committed transfer."""

        abort(500)
