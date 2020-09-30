from flask import current_app, request, g
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_creditors.schemas import examples, CreditorSchema, WalletSchema, LogEntriesPageSchema, \
    LogPaginationParamsSchema, AccountsListSchema, TransfersListSchema, PinInfoSchema
from swpt_creditors import procedures
from .common import context, process_headers
from .specs import CID
from . import specs


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Get information about creditors, create new creditors.",
)
creditors_api.before_request(process_headers)


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

        return procedures.get_active_creditor(creditorId, join_pin=True) or abort(404)


@creditors_api.route('/<i64:creditorId>/', parameters=[CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(CreditorSchema(context=context))
    @creditors_api.doc(operationId='getCreditor')
    def get(self, creditorId):
        """Return a creditor."""

        return procedures.get_active_creditor(creditorId) or abort(403)


@creditors_api.route('/<i64:creditorId>/pin', parameters=[CID])
class PinInfoEndpoint(MethodView):
    @creditors_api.response(PinInfoSchema(context=context))
    @creditors_api.doc(operationId='getPinInfo')
    def get(self, creditorId):
        """Return creditor's PIN information."""

        return procedures.get_pin(creditorId) or abort(404)

    @creditors_api.arguments(PinInfoSchema)
    @creditors_api.response(PinInfoSchema(context=context))
    @creditors_api.doc(operationId='updatePinInfo',
                       responses={403: specs.FORBIDDEN_OPERATION,
                                  409: specs.UPDATE_CONFLICT})
    def patch(self, pin_info, creditorId):
        """Update creditor's PIN information.

        **Note:** This is a potentially dangerous operation which may
        require a PIN. Also, normally this is an idempotent operation,
        but when an incorrect PIN is supplied, repeating the operation
        may result in the creditor's PIN being blocked.

        """

        try:
            if not g.pin_reset_mode:
                procedures.verify_pin_value(
                    creditor_id=creditorId,
                    value=pin_info.get('optional_pin'),
                    max_failed_attempts=int(current_app.config['APP_PIN_MAX_FAILED_ATTEMPTS']),
                )
            pin = procedures.update_pin(
                creditor_id=creditorId,
                status_name=pin_info['status_name'],
                new_pin=pin_info.get('optional_new_pin'),
                latest_update_id=pin_info['latest_update_id'],
            )
        except procedures.WrongPinValue:
            abort(403)
        except procedures.CreditorDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        return pin


@creditors_api.route('/<i64:creditorId>/log', parameters=[CID])
class LogEntriesEndpoint(MethodView):
    @creditors_api.arguments(LogPaginationParamsSchema, location='query')
    @creditors_api.response(LogEntriesPageSchema(context=context), example=examples.LOG_ENTRIES_EXAMPLE)
    @creditors_api.doc(operationId='getLogPage')
    def get(self, params, creditorId):
        """Return a collection of creditor's recent log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains recent log entries. The
        returned fragment, and all the subsequent fragments, will be
        sorted in chronological order (smaller `entryId`s go
        first).

        """

        n = int(current_app.config['APP_LOG_ENTRIES_PER_PAGE'])
        try:
            log_entries, last_log_entry_id = procedures.get_log_entries(
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


@creditors_api.route('/<i64:creditorId>/accounts-list', parameters=[CID])
class AccountsListEndpoint(MethodView):
    @creditors_api.response(AccountsListSchema(context=context), example=examples.ACCOUNTS_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getAccountsList')
    def get(self, creditorId):
        """Return a paginated list of links to all accounts belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        return procedures.get_active_creditor(creditorId) or abort(404)


@creditors_api.route('/<i64:creditorId>/transfers-list', parameters=[CID])
class TransfersListEndpoint(MethodView):
    @creditors_api.response(TransfersListSchema(context=context), example=examples.TRANSFERS_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getTransfersList')
    def get(self, creditorId):
        """Return a paginated list of links to all transfers belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        return procedures.get_active_creditor(creditorId) or abort(404)
