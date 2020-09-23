from flask import current_app, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_creditors.schemas import examples, CreditorSchema, CreditorReservationRequestSchema, \
    CreditorReservationSchema, CreditorActivationRequestSchema, CreditorDeactivationRequestSchema, \
    ObjectReferencesPageSchema
from swpt_creditors import procedures
from .common import context, path_builder
from .specs import CID
from . import specs


admin_api = Blueprint(
    'admin',
    __name__,
    url_prefix='/creditors',
    description="View creditors list, create new creditors.",
)


@admin_api.route('/<i64:creditorId>/enumerate', parameters=[CID])
class EnumerateCreditorsEndpoint(MethodView):
    @admin_api.response(ObjectReferencesPageSchema(context=context), example=examples.LOG_ENTRIES_EXAMPLE)
    @admin_api.doc(operationId='getCreditorsPage')
    def get(self, creditorId):
        """Return a collection of creditor URIs.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains creditor URIs, starting from
        the creditor with the ID specified in the path. The returned
        fragment, and all the subsequent fragments, will be sorted by
        the creditor's ID.

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


@admin_api.route('/<i64:creditorId>/reserve', parameters=[CID])
class ReserveCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorReservationRequestSchema)
    @admin_api.response(CreditorReservationSchema(context=context))
    @admin_api.doc(operationId='reserveCreditor',
                   responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_creation_request, creditorId):
        """Try to reserve a creditor ID.

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


@admin_api.route('/<i64:creditorId>/activate', parameters=[CID])
class ActivateCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorActivationRequestSchema)
    @admin_api.response(CreditorSchema(context=context))
    @admin_api.doc(operationId='activateCreditor',
                   responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_activation_request, creditorId):
        """Activate a creditor."""

        activation_code = creditor_activation_request.get('optional_activation_code')
        try:
            if activation_code is None:
                activation_code = procedures.reserve_creditor(creditorId).activation_code
            creditor = procedures.activate_creditor(creditorId, activation_code)
        except procedures.CreditorExists:
            abort(409)
        except procedures.InvalidActivationCode:
            abort(422, errors={'json': {'activationCode': ['Invalid code.']}})
        except procedures.InvalidCreditor:  # pragma: no cover
            abort(500, message='The agent is not responsible for this creditor.')

        return creditor


@admin_api.route('/<i64:creditorId>/deactivate', parameters=[CID])
class DeactivateCreditorEndpoint(MethodView):
    @admin_api.arguments(CreditorDeactivationRequestSchema)
    @admin_api.response(code=204)
    @admin_api.doc(operationId='deactivateCreditor')
    def post(self, creditor_deactivation_request, creditorId):
        """Deactivate a creditor."""

        procedures.deactivate_creditor(creditorId)
