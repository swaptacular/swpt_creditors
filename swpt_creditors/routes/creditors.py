from flask import current_app, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_creditors.schemas import (
    CreditorSchema, CreditorCreationRequestSchema, WalletSchema,
    LogEntriesPageSchema, LogPaginationParamsSchema, AccountListSchema, TransferListSchema,
)
from swpt_creditors.specs import CID
from swpt_creditors import specs
from swpt_creditors import procedures
from .common import context


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Get information about creditors, create new creditors.",
)


@creditors_api.route('/<i64:creditorId>/', parameters=[CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(CreditorSchema(context=context))
    @creditors_api.doc(operationId='getCreditor')
    def get(self, creditorId):
        """Return a creditor."""

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(403)
        return creditor

    @creditors_api.arguments(CreditorCreationRequestSchema)
    @creditors_api.response(CreditorSchema(context=context), code=202)
    @creditors_api.doc(operationId='createCreditor',
                       responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_creation_request, creditorId):
        """Try to create a new creditor. Requires special privileges.

        ---
        Must fail if the creditor already exists.

        """

        try:
            creditor = procedures.create_new_creditor(creditorId, activate=creditor_creation_request['activate'])
        except procedures.CreditorExists:
            abort(409)
        return creditor

    @creditors_api.arguments(CreditorSchema)
    @creditors_api.response(CreditorSchema(context=context))
    @creditors_api.doc(operationId='updateCreditor',
                       responses={409: specs.UPDATE_CONFLICT})
    def patch(self, creditor, creditorId):
        """Update a creditor.

        **Note:** This is an idempotent operation.

        """

        try:
            creditor = procedures.update_creditor(creditorId, latest_update_id=creditor['latest_update_id'])
        except procedures.CreditorDoesNotExist:
            abort(403)
        except procedures.UpdateConflict:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        return creditor


@creditors_api.route('/<i64:creditorId>/wallet', parameters=[CID])
class WalletEndpoint(MethodView):
    @creditors_api.response(WalletSchema(context=context))
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
    @creditors_api.response(LogEntriesPageSchema(context=context), example=specs.LOG_ENTRIES_EXAMPLE)
    @creditors_api.doc(operationId='getLogPage')
    def get(self, params, creditorId):
        """Return a collection of creditor's recent log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains recent log entries. The
        returned fragment, and all the subsequent fragments, will be
        sorted in chronological order (smaller `entryId`s go
        first).

        """

        n = current_app.config['APP_LOG_ENTRIES_PER_PAGE']
        try:
            log_entries, last_log_entry_id = procedures.get_creditor_log_entries(
                creditorId,
                count=n,
                prev=params['prev'],
            )
        except procedures.CreditorDoesNotExist:
            abort(404)

        if len(log_entries) < n:
            # The last page does not have a 'next' link.
            return {
                'uri': request.full_path,
                'items': log_entries,
                'forthcoming': f'?prev={last_log_entry_id}',
            }

        return {
            'uri': request.full_path,
            'items': log_entries,
            'next': f'?prev={log_entries[-1].entry_id}',
        }


@creditors_api.route('/<i64:creditorId>/account-list', parameters=[CID])
class AccountListEndpoint(MethodView):
    @creditors_api.response(AccountListSchema(context=context), example=specs.ACCOUNT_LIST_EXAMPLE)
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
    @creditors_api.response(TransferListSchema(context=context), example=specs.TRANSFER_LIST_EXAMPLE)
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
