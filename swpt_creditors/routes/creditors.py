from datetime import timedelta
from flask import current_app, request, g, redirect, url_for
from flask.views import MethodView
from flask_smorest import abort
from swpt_creditors.schemas import (
    examples,
    CreditorSchema,
    WalletSchema,
    LogEntriesPageSchema,
    LogPaginationParamsSchema,
    AccountsListSchema,
    TransfersListSchema,
    PinInfoSchema,
)
from swpt_creditors import procedures
from .common import context, ensure_creditor_permissions, Blueprint
from .specs import CID
from . import specs


creditors_api = Blueprint(
    "creditors",
    __name__,
    url_prefix="/creditors",
    description="""**Obtain information about existing creditors, change
    creditors' PINs.** There are two important concepts here: Each creditor
    has a *"wallet"*, which contains references to various kinds of
    information about the creditor (like creditor's list of accounts
    and transfers). The other important concept is the creditor's
    *"log"*. Whenever there is new information that the creditor
    should be aware of, a record will be added to the creditor's log,
    referring to the created/updated/deleted object. The purpose of
    the log is to allow the clients of the API to reliably synchronize
    their local databases with the server, simply by following the
    "log".""",
)
creditors_api.before_request(ensure_creditor_permissions)


@creditors_api.route("/.wallet")
class RedirectToWalletEndpoint(MethodView):
    @creditors_api.response(204)
    @creditors_api.doc(
        operationId="redirectToWallet",
        security=specs.SCOPE_ACCESS_READONLY,
        responses={204: specs.WALLET_DOES_NOT_EXIST, 303: specs.WALLET_EXISTS},
    )
    def get(self):
        """Redirect to the creditor's wallet."""

        creditorId = g.creditor_id
        if creditorId is not None:
            location = url_for(
                "creditors.WalletEndpoint",
                _external=True,
                creditorId=creditorId,
            )
            return redirect(location, code=303)


@creditors_api.route("/<i64:creditorId>/", parameters=[CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(200, CreditorSchema(context=context))
    @creditors_api.doc(
        operationId="getCreditor", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId):
        """Return a creditor."""

        return procedures.get_active_creditor(creditorId) or abort(403)


@creditors_api.route("/<i64:creditorId>/wallet", parameters=[CID])
class WalletEndpoint(MethodView):
    @creditors_api.response(200, WalletSchema(context=context))
    @creditors_api.doc(
        operationId="getWallet", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId):
        """Return creditor's wallet.

        The creditor's wallet "contains" all creditor's accounts,
        pending transfers, and recent events (the log). In short: it
        is the gateway to all objects and operations in the API.

        """

        return procedures.get_active_creditor(
            creditorId, join_pin=True
        ) or abort(404)


@creditors_api.route("/<i64:creditorId>/pin", parameters=[CID])
class PinInfoEndpoint(MethodView):
    @creditors_api.response(200, PinInfoSchema(context=context))
    @creditors_api.doc(
        operationId="getPinInfo", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId):
        """Return creditor's PIN information."""

        return procedures.get_pin_info(creditorId) or abort(404)

    @creditors_api.arguments(PinInfoSchema)
    @creditors_api.response(200, PinInfoSchema(context=context))
    @creditors_api.doc(
        operationId="updatePinInfo",
        security=specs.SCOPE_ACCESS_MODIFY,
        responses={403: specs.FORBIDDEN_OPERATION, 409: specs.UPDATE_CONFLICT},
    )
    def patch(self, pin_info, creditorId):
        """Update creditor's PIN information.

        **Note:** This is a potentially dangerous operation which may
        require a PIN. Also, normally this is an idempotent operation,
        but when an incorrect PIN is supplied, repeating the operation
        may result in the creditor's PIN being blocked.

        """

        try:
            return procedures.update_pin_info(
                creditor_id=creditorId,
                status_name=pin_info["status_name"],
                secret=current_app.config["PIN_PROTECTION_SECRET"],
                new_pin_value=pin_info.get("optional_new_pin_value"),
                latest_update_id=pin_info["latest_update_id"],
                pin_reset_mode=g.pin_reset_mode,
                pin_value=pin_info.get("optional_pin"),
                pin_failures_reset_interval=timedelta(
                    days=current_app.config["APP_PIN_FAILURES_RESET_DAYS"]
                ),
            )
        except procedures.WrongPinValue:
            abort(403)
        except procedures.CreditorDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(
                409, errors={"json": {"latestUpdateId": ["Incorrect value."]}}
            )


@creditors_api.route("/<i64:creditorId>/log", parameters=[CID])
class LogEntriesEndpoint(MethodView):
    @creditors_api.arguments(LogPaginationParamsSchema, location="query")
    @creditors_api.response(
        200,
        LogEntriesPageSchema(context=context),
        example=examples.LOG_ENTRIES_EXAMPLE,
    )
    @creditors_api.doc(
        operationId="getLogPage", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, params, creditorId):
        """Return a collection of creditor's recent log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains recent log entries. The
        returned fragment, and all the subsequent fragments, will be
        sorted in chronological order (smaller `entryId`s go
        first).

        **Note:** The number of items in the returned fragment should
        be small enough, so that all the items in the fragment can be
        processed and saved to the client's local database in a single
        database transaction.

        """

        n = current_app.config["APP_LOG_ENTRIES_PER_PAGE"]
        try:
            log_entries, last_log_entry_id = procedures.get_log_entries(
                creditorId,
                count=n,
                prev=params["prev"],
            )
        except procedures.CreditorDoesNotExist:
            abort(404)

        if len(log_entries) < n:
            # The last page does not have a 'next' link.
            return {
                "uri": request.full_path,
                "items": log_entries,
                "forthcoming": f"?prev={last_log_entry_id}",
            }

        return {
            "uri": request.full_path,
            "items": log_entries,
            "next": f"?prev={log_entries[-1].entry_id}",
        }


@creditors_api.route("/<i64:creditorId>/accounts-list", parameters=[CID])
class AccountsListEndpoint(MethodView):
    @creditors_api.response(
        200,
        AccountsListSchema(context=context),
        example=examples.ACCOUNTS_LIST_EXAMPLE,
    )
    @creditors_api.doc(
        operationId="getAccountsList", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId):
        """Return a paginated list of links to all accounts belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        return procedures.get_active_creditor(creditorId) or abort(404)


@creditors_api.route("/<i64:creditorId>/transfers-list", parameters=[CID])
class TransfersListEndpoint(MethodView):
    @creditors_api.response(
        200,
        TransfersListSchema(context=context),
        example=examples.TRANSFERS_LIST_EXAMPLE,
    )
    @creditors_api.doc(
        operationId="getTransfersList", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId):
        """Return a paginated list of links to all transfers belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        return procedures.get_active_creditor(creditorId) or abort(404)
