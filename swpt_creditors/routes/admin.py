from random import randint
from flask import current_app, request, g
from flask.views import MethodView
from flask_smorest import abort
from swpt_creditors.schemas import (
    examples,
    CreditorSchema,
    CreditorReservationRequestSchema,
    CreditorReservationSchema,
    CreditorActivationRequestSchema,
    CreditorDeactivationRequestSchema,
    ObjectReferencesPageSchema,
)
from swpt_creditors import procedures
from swpt_creditors.schemas import type_registry, CreditorsListSchema
from swpt_creditors.models import MIN_INT64, is_valid_creditor_id
from .common import context, path_builder, ensure_admin, Blueprint
from .specs import CID
from . import specs


admin_api = Blueprint(
    "admin",
    __name__,
    url_prefix="/creditors",
    description="""**View creditors list, create new creditors, deactivate
    inactive creditors.** The creation of new creditors can optionally be
    done in two-phases: First a creditor ID can be *reserved*, and only
    then, the creditor can be *activated*. This is useful when the
    client wants to know the new creditor ID in advance. If this is
    not needed, the creditor can also be activated directly, by a
    single request.""",
)
admin_api.before_request(ensure_admin)


@admin_api.route("/.creditor-reserve")
class RandomCreditorReserveEndpoint(MethodView):
    @admin_api.arguments(CreditorReservationRequestSchema)
    @admin_api.response(200, CreditorReservationSchema(context=context))
    @admin_api.doc(
        operationId="reserveRandomCreditor",
        security=specs.SCOPE_ACTIVATE,
        responses={409: specs.CONFLICTING_CREDITOR},
    )
    def post(self, creditor_reservation_request):
        """Reserve an auto-generated creditor ID.

        **Note:** The reserved creditor ID will be a random valid
        creditor ID.

        """
        min_creditor_id = current_app.config["MIN_CREDITOR_ID"]
        max_creditor_id = current_app.config["MAX_CREDITOR_ID"]
        for _ in range(100):
            creditor_id = randint(min_creditor_id, max_creditor_id)
            if not is_valid_creditor_id(creditor_id):  # pragma: no cover
                abort(
                    500,
                    message=(
                        "The /.creditor-reserve endpoint does not support"
                        " shards."
                    ),
                )
            try:
                creditor = procedures.reserve_creditor(creditor_id)
                break
            except procedures.CreditorExists:  # pragma: no cover
                pass
        else:  # pragma: no cover
            abort(500, message="Can not generate a valid creditor ID.")

        return creditor


@admin_api.route("/.list")
class CreditorsListEndpoint(MethodView):
    @admin_api.response(
        200, CreditorsListSchema, example=examples.CREDITORS_LIST_EXAMPLE
    )
    @admin_api.doc(
        operationId="getCreditorsList", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self):
        """Return a paginated list of links to all active creditors."""

        return {
            "uri": path_builder.creditors_list(),
            "items_type": type_registry.object_reference,
            "first": path_builder.creditor_enumerate(creditorId=MIN_INT64),
        }


@admin_api.route("/<i64:creditorId>/enumerate", parameters=[CID])
class CreditorEnumerateEndpoint(MethodView):
    @admin_api.response(
        200,
        ObjectReferencesPageSchema(context=context),
        example=examples.CREDITOR_LINKS_EXAMPLE,
    )
    @admin_api.doc(
        operationId="getCreditorsPage", security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, creditorId):
        """Return a collection of active creditors.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all active
        creditors on the server. The returned fragment, and all the
        subsequent fragments, will be sorted by creditor ID, starting
        from the `creditorID` specified in the path. The sorting order
        is implementation-specific.

        **Note:** To obtain references to all active creditors, the
        client should start with the creditor ID that precedes all
        other IDs in the sorting order.

        """

        n = current_app.config["APP_CREDITORS_PER_PAGE"]
        creditor_ids, next_creditor_id = procedures.get_creditor_ids(
            start_from=creditorId, count=n
        )
        creditor_uris = [
            {"uri": path_builder.creditor(creditorId=creditor_id)}
            for creditor_id in creditor_ids
            if is_valid_creditor_id(creditor_id)
        ]

        if next_creditor_id is None:
            # The last page does not have a 'next' link.
            return {
                "uri": request.full_path,
                "items": creditor_uris,
            }

        return {
            "uri": request.full_path,
            "items": creditor_uris,
            "next": path_builder.creditor_enumerate(
                creditorId=next_creditor_id
            ),
        }


@admin_api.route("/<i64:creditorId>/reserve", parameters=[CID])
class CreditorReserveEndpoint(MethodView):
    @admin_api.arguments(CreditorReservationRequestSchema)
    @admin_api.response(200, CreditorReservationSchema(context=context))
    @admin_api.doc(
        operationId="reserveCreditor",
        security=specs.SCOPE_ACTIVATE,
        responses={409: specs.CONFLICTING_CREDITOR},
    )
    def post(self, creditor_reservation_request, creditorId):
        """Try to reserve a specific creditor ID.

        **Note:** The reserved creditor ID will be the same as the
        `creditorId` specified in the path.

        ---
        Will fail if the creditor already exists.

        """

        if not is_valid_creditor_id(creditorId):  # pragma: no cover
            abort(404)

        try:
            creditor = procedures.reserve_creditor(creditorId)
        except procedures.CreditorExists:
            abort(409)

        return creditor


@admin_api.route("/<i64:creditorId>/activate", parameters=[CID])
class CreditorActivateEndpoint(MethodView):
    @admin_api.arguments(CreditorActivationRequestSchema)
    @admin_api.response(200, CreditorSchema(context=context))
    @admin_api.doc(
        operationId="activateCreditor",
        security=specs.SCOPE_ACTIVATE,
        responses={409: specs.CONFLICTING_CREDITOR},
    )
    def post(self, creditor_activation_request, creditorId):
        """Activate a creditor."""

        if not is_valid_creditor_id(creditorId):  # pragma: no cover
            abort(404)

        reservation_id = creditor_activation_request.get(
            "optional_reservation_id"
        )
        try:
            if reservation_id is None:
                reservation_id = str(
                    procedures.reserve_creditor(creditorId).reservation_id
                )
                assert reservation_id is not None
            creditor = procedures.activate_creditor(creditorId, reservation_id)
        except procedures.CreditorExists:
            abort(409)
        except procedures.InvalidReservationId:
            abort(422, errors={"json": {"reservationId": ["Invalid ID."]}})

        return creditor


@admin_api.route("/<i64:creditorId>/deactivate", parameters=[CID])
class CreditorDeactivateEndpoint(MethodView):
    @admin_api.arguments(CreditorDeactivationRequestSchema)
    @admin_api.response(204)
    @admin_api.doc(
        operationId="deactivateCreditor", security=specs.SCOPE_DEACTIVATE
    )
    def post(self, creditor_deactivation_request, creditorId):
        """Deactivate a creditor."""

        if not is_valid_creditor_id(creditorId):  # pragma: no cover
            abort(404)

        if not g.superuser:
            abort(403)

        procedures.deactivate_creditor(creditorId)
