from urllib.parse import urljoin
from flask import redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints
from .schemas import (
    CreditorCreationOptionsSchema, CreditorSchema, AccountsCollectionSchema, AccountCreationRequestSchema,
    AccountSchema, AccountRecordSchema, AccountRecordConfigSchema,
    CommittedTransferSchema, LedgerEntryListSchema,
)
from . import specs
from . import procedures

CONTEXT = {
    'Creditor': 'creditors.CreditorEndpoint',
    'Account': 'creditors.AccountEndpoint',
    'TransfersCollection': 'transfers.TransfersCollectionEndpoint',
    'Transfer': 'transfers.TransferEndpoint',
    'AccountsCollection': 'accounts.AccountCollectionEndpoint',
    'AccountRecord': 'accounts.AccountRecordEndpoint',
}


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Obtain public information about accounts and creditors, create new creditors.",
)


@creditors_api.route('/<i64:creditorId>/', parameters=[specs.CREDITOR_ID])
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


@creditors_api.route('/<i64:creditorId>/debtors/<i64:debtorId>', parameters=[specs.CREDITOR_ID, specs.DEBTOR_ID])
class AccountEndpoint(MethodView):
    @creditors_api.response(AccountSchema(context=CONTEXT))
    @creditors_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return public information about an account."""

        account = None
        if not account:
            abort(404)
        return account, {'Cache-Control': 'max-age=86400'}


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="View, create, update and delete creditors' accounts.",
)


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[specs.CREDITOR_ID])
class AccountCollectionEndpoint(MethodView):
    @accounts_api.response(AccountsCollectionSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.CREDITOR_DOES_NOT_EXIST})
    def get(self, creditorId):
        """Return the creditor's collection of account records."""

        try:
            debtor_ids = procedures.get_account_dedtor_ids(creditorId)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        return AccountsCollectionSchema(creditor_id=creditorId, items=debtor_ids)

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


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/', parameters=[specs.CREDITOR_ID, specs.DEBTOR_ID])
class AccountRecordEndpoint(MethodView):
    @accounts_api.response(AccountRecordSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
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


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/config', parameters=[specs.CREDITOR_ID, specs.DEBTOR_ID])
class AccountRecordConfigEndpoint(MethodView):
    @accounts_api.response(AccountRecordConfigSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId):
        """Return account's configuration."""

        abort(500)

    @accounts_api.arguments(AccountRecordConfigSchema)
    @accounts_api.response(AccountRecordConfigSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST,
                                 409: specs.ACCOUNT_UPDATE_CONFLICT})
    def patch(self, config_update_request, creditorId, debtorId):
        """Update account's configuration.

        **Note:** This operation is idempotent.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries',
                    parameters=[specs.CREDITOR_ID, specs.DEBTOR_ID, specs.FIRST, specs.AFTER])
class AccountLedgerEntryListEndpoint(MethodView):
    @accounts_api.response(LedgerEntryListSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId, first, after):
        """Return a paginated list of ledger entries for a given account.

        The paginated list will be sorted in reverse-chronological
        order (bigger entry IDs go first). The entries will constitute
        a singly linked list, each entry (except the most ancient one)
        referring to its ancestor.

        """

        abort(500)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<i64:transferSeqnum>',
                    parameters=[specs.CREDITOR_ID, specs.DEBTOR_ID])
class AccountTransferEndpoint(MethodView):
    @accounts_api.response(CommittedTransferSchema(context=CONTEXT))
    @accounts_api.doc(responses={404: specs.ACCOUNT_DOES_NOT_EXIST})
    def get(self, creditorId, debtorId, transferSeqnum):
        """Return information about a committed transfer."""

        abort(500)
