from flask import current_app, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_creditors.schemas import examples, CreditorSchema, CreditorReservationRequestSchema, \
    CreditorReservationSchema, CreditorActivationRequestSchema, CreditorDeactivationRequestSchema, \
    ObjectReferencesPageSchema
from swpt_creditors import procedures
from swpt_creditors.schemas import type_registry, CreditorsListSchema
from swpt_creditors.models import MIN_INT64
from .common import context, path_builder
from .specs import CID
from . import specs


admin_api = Blueprint(
    'admin',
    __name__,
    url_prefix='/',
    description="View creditors list, create new creditors.",
)


@admin_api.route('/creditors-reserve')
class ReserveRandomCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorReservationRequestSchema)
    @admin_api.response(CreditorReservationSchema(context=context))
    @admin_api.doc(operationId='reserveRandomCreditor',
                   responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_reservation_request):
        """Reserve an auto-generated creditor ID.

        **Note:** The reserved creditor ID will be a random valid
        creditor ID.

        """

        for _ in range(100):
            creditor_id = procedures.generate_new_creditor_id()
            try:
                creditor = procedures.reserve_creditor(creditor_id, verify_correctness=False)
                break
            except procedures.CreditorExists:  # pragma: no cover
                pass
        else:  # pragma: no cover
            abort(500, message='Can not generate a valid creditor ID.')

        return creditor


@admin_api.route('/creditors-list')
class CreditorsListEndpoint(MethodView):
    @admin_api.response(CreditorsListSchema, example=examples.CREDITORS_LIST_EXAMPLE)
    @admin_api.doc(operationId='getCreditorsList')
    def get(self):
        """Return a paginated list of links to all active creditors."""

        return {
            'uri': path_builder.creditors_list(),
            'items_type': type_registry.object_reference,
            'first': path_builder.enumerate_creditors(creditorId=MIN_INT64)
        }


@admin_api.route('/creditors/<i64:creditorId>/enumerate', parameters=[CID])
class EnumerateCreditorsEndpoint(MethodView):
    @admin_api.response(ObjectReferencesPageSchema(context=context), example=examples.CREDITOR_LINKS_EXAMPLE)
    @admin_api.doc(operationId='getCreditorsPage')
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

        n = int(current_app.config['APP_CREDITORS_PER_PAGE'])
        creditor_ids, next_creditor_id = procedures.get_creditor_ids(start_from=creditorId, count=n)
        creditor_uris = [{'uri': path_builder.creditor(creditorId=creditor_id)} for creditor_id in creditor_ids]

        if next_creditor_id is None:
            # The last page does not have a 'next' link.
            return {
                'uri': request.full_path,
                'items': creditor_uris,
            }

        return {
            'uri': request.full_path,
            'items': creditor_uris,
            'next': path_builder.enumerate_creditors(creditorId=next_creditor_id),
        }


@admin_api.route('/creditors/<i64:creditorId>/reserve', parameters=[CID])
class ReserveCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorReservationRequestSchema)
    @admin_api.response(CreditorReservationSchema(context=context))
    @admin_api.doc(operationId='reserveCreditor',
                   responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_reservation_request, creditorId):
        """Try to reserve a specific creditor ID.

        **Note:** The reserved creditor ID will be the same as the
        `creditorId` specified in the path.

        ---
        Must fail if the creditor already exists.

        """

        try:
            creditor = procedures.reserve_creditor(creditorId)
        except procedures.CreditorExists:
            abort(409)
        except procedures.InvalidCreditor:  # pragma: no cover
            abort(500, message='The agent is not responsible for this creditor.')

        return creditor


@admin_api.route('/creditors/<i64:creditorId>/activate', parameters=[CID])
class ActivateCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorActivationRequestSchema)
    @admin_api.response(CreditorSchema(context=context))
    @admin_api.doc(operationId='activateCreditor',
                   responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_activation_request, creditorId):
        """Activate a creditor."""

        reservation_id = creditor_activation_request.get('optional_reservation_id')
        try:
            if reservation_id is None:
                reservation_id = procedures.reserve_creditor(creditorId).reservation_id
                assert reservation_id is not None
            creditor = procedures.activate_creditor(creditorId, reservation_id)
        except procedures.CreditorExists:
            abort(409)
        except procedures.InvalidReservationId:
            abort(422, errors={'json': {'reservationId': ['Invalid ID.']}})
        except procedures.InvalidCreditor:  # pragma: no cover
            abort(500, message='The agent is not responsible for this creditor.')

        return creditor


@admin_api.route('/creditors/<i64:creditorId>/deactivate', parameters=[CID])
class DeactivateCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorDeactivationRequestSchema)
    @admin_api.response(code=204)
    @admin_api.doc(operationId='deactivateCreditor')
    def post(self, creditor_deactivation_request, creditorId):
        """Deactivate a creditor."""

        procedures.deactivate_creditor(creditorId)
