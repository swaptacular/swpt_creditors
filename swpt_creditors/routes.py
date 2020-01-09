from urllib.parse import urljoin
from flask import redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib import endpoints
from .schemas import CreditorCreationOptionsSchema, CreditorSchema
from . import specs
from . import procedures

CONTEXT = {
    'Creditor': 'creditors.CreditorEndpoint',
    'TransfersCollection': 'transfers.TransfersCollectionEndpoint',
    'Transfer': 'transfers.TransferEndpoint'
}


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Obtain public information about creditors and create new creditors.",
)


@creditors_api.route('/<i64:creditorId>', parameters=[specs.CREDITOR_ID])
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
