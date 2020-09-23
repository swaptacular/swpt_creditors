from marshmallow import Schema, fields, validate
from swpt_lib.utils import i64_to_u64
from swpt_creditors.models import MIN_INT64, MAX_INT64
from .common import type_registry, ValidateTypeMixin


class CreditorReservationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing=type_registry.creditor_reservation_request,
        load_only=True,
        description='The type of this object.',
        example='CreditorReservationRequest',
    )


class CreditorReservationSchema(ValidateTypeMixin, Schema):
    type = fields.Function(
        lambda obj: type_registry.creditor_reservation,
        required=True,
        type='string',
        description='The type of this object.',
        example='CreditorReservation',
    )
    created_at_ts = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the reservation was created.',
    )
    reservationId = fields.Function(
        lambda obj: obj.reservation_id or 0,
        required=True,
        description='A number that will be required when activating the creditor.',
        type='integer',
        format='int64',
        example=12345,
    )
    creditorId = fields.Function(
        lambda obj: str(i64_to_u64(obj.creditor_id)),
        required=True,
        type='string',
        description='The creditor ID that has been reserved.',
        example='1',
    )
    validUntil = fields.Method(
        'get_valid_until_string',
        required=True,
        type='string',
        format='date-time',
        description='The reservation will not be valid after this moment.',
    )

    def get_valid_until_string(self, obj) -> str:
        calc_reservation_deadline = self.context['calc_reservation_deadline']
        return calc_reservation_deadline(obj.created_at_ts).isoformat()


class CreditorActivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing=type_registry.creditor_activation_request,
        load_only=True,
        description='The type of this object.',
        example='CreditorActivationRequest',
    )
    optional_reservation_id = fields.Integer(
        load_only=True,
        data_key='reservationId',
        format='int64',
        description='When this field is present, the server will try to activate an existing '
                    'reservation with matching `creditorID` and `reservationID`. When this '
                    'field is not present, the server will try to reserve the creditor ID '
                    'specified in the path, and activate it at once.',
        example=12345,
    )


class CreditorDeactivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing=type_registry.creditor_deactivation_request,
        load_only=True,
        description='The type of this object.',
        example='CreditorDeactivationRequest',
    )
