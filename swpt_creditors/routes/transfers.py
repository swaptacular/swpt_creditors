from flask import redirect, url_for
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib.swpt_uris import parse_account_uri
from swpt_creditors.schemas import (
    TransferCreationRequestSchema, TransferSchema, CommittedTransferSchema,
    TransferCancelationRequestSchema, ObjectReferencesPageSchema, PaginationParametersSchema,
)
from swpt_creditors.specs import DID, CID, TID, TRANSFER_UUID
from swpt_creditors import specs
from swpt_creditors import procedures
from swpt_creditors import inspect_ops
from .common import context, parse_transfer_slug


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/creditors',
    description="Make transfers from one account to another account.",
)


@transfers_api.route('/<i64:creditorId>/transfers/', parameters=[CID])
class TransfersEndpoint(MethodView):
    @transfers_api.arguments(PaginationParametersSchema, location='query')
    @transfers_api.response(ObjectReferencesPageSchema(context=context), example=specs.TRANSFER_LINKS_EXAMPLE)
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
    @transfers_api.response(TransferSchema(context=context), code=201, headers=specs.LOCATION_HEADER)
    @transfers_api.doc(operationId='createTransfer',
                       responses={303: specs.TRANSFER_EXISTS,
                                  403: specs.DENIED_TRANSFER,
                                  409: specs.TRANSFER_CONFLICT})
    def post(self, transfer_creation_request, creditorId):
        """Initiate a transfer.

        **Note:** This is an idempotent operation.

        """

        try:
            debtorId, recipient = parse_account_uri(transfer_creation_request['recipient']['uri'])
        except ValueError:
            abort(422, errors={'json': {'recipient': {'uri': ['The URI can not be recognized.']}}})

        uuid = transfer_creation_request['transfer_uuid']
        location = url_for('transfers.TransferEndpoint', _external=True, creditorId=creditorId, transferUuid=uuid)
        try:
            inspect_ops.allow_direct_transfer_creation(creditorId, debtorId)
            transfer = procedures.initiate_transfer(
                creditor_id=creditorId,
                transfer_uuid=uuid,
                debtor_id=debtorId,
                amount=transfer_creation_request['amount'],
                recipient=recipient,
                note=transfer_creation_request['note'],
                min_interest_rate=transfer_creation_request['options']['min_interest_rate'],
                deadline=transfer_creation_request['options'].get('optional_deadline'),
            )
        except inspect_ops.ForbiddenOperation:  # pragma: no cover
            abort(403)
        except procedures.CreditorDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409)
        except procedures.TransferExists:
            return redirect(location, code=303)

        inspect_ops.register_direct_transfer_creation(creditorId, debtorId)
        return transfer, {'Location': location}


@transfers_api.route('/<i64:creditorId>/transfers/<uuid:transferUuid>', parameters=[CID, TRANSFER_UUID])
class TransferEndpoint(MethodView):
    @transfers_api.response(TransferSchema(context=context))
    @transfers_api.doc(operationId='getTransfer')
    def get(self, creditorId, transferUuid):
        """Return a transfer."""

        return procedures.get_direct_transfer(creditorId, transferUuid) or abort(404)

    @transfers_api.arguments(TransferCancelationRequestSchema)
    @transfers_api.response(TransferSchema(context=context))
    @transfers_api.doc(operationId='cancelTransfer',
                       responses={403: specs.TRANSFER_CANCELLATION_FAILURE})
    def post(self, cancel_transfer_request, creditorId, transferUuid):
        """Try to cancel a transfer.

        **Note:** This is an idempotent operation.

        """

        try:
            transfer = procedures.cancel_transfer(creditorId, transferUuid)
        except procedures.ForbiddenTransferCancellation:
            abort(403)
        except procedures.TransferDoesNotExist:
            abort(404)
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
    @transfers_api.response(CommittedTransferSchema(context=context))
    @transfers_api.doc(operationId='getCommittedTransfer')
    def get(self, creditorId, debtorId, transferId):
        """Return information about sent or received transfer."""

        try:
            creation_date, transfer_number = parse_transfer_slug(transferId)
        except ValueError:
            abort(404)

        abort(500)
