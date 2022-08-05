from marshmallow import Schema, fields, post_dump
from swpt_pythonlib.utils import i64_to_u64
from .common import type_registry, ValidateTypeMixin, PaginatedListSchema, URI_DESCRIPTION, TYPE_DESCRIPTION


class CreditorsListSchema(PaginatedListSchema):
    uri = fields.String(
        required=True,
        dump_only=True,
        format='uri-reference',
        description=URI_DESCRIPTION,
        example='/creditors/.list',
    )
    type = fields.Function(
        lambda obj: type_registry.creditors_list,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
        example='CreditorsList',
    )

    @post_dump
    def assert_required_fields(self, obj, many):
        assert 'uri' in obj
        assert 'itemsType' in obj
        assert 'first' in obj
        return obj


class CreditorReservationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing=type_registry.creditor_reservation_request,
        load_only=True,
        description=TYPE_DESCRIPTION,
        example='CreditorReservationRequest',
    )


class CreditorReservationSchema(ValidateTypeMixin, Schema):
    type = fields.Function(
        lambda obj: type_registry.creditor_reservation,
        required=True,
        type='string',
        description=TYPE_DESCRIPTION,
        example='CreditorReservation',
    )
    created_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key='createdAt',
        description='The moment at which the reservation was created.',
    )
    reservation_id = fields.Function(
        lambda obj: obj.reservation_id or 0,
        required=True,
        data_key='reservationId',
        type='integer',
        format='int64',
        description='A number that will be needed in order to activate the creditor.',
        example=12345,
    )
    creditor_id = fields.Function(
        lambda obj: str(i64_to_u64(obj.creditor_id)),
        required=True,
        data_key='creditorId',
        type='string',
        pattern='^[0-9A-Za-z_=-]{1,64}$',
        description='The reserved creditor ID.',
        example='1',
    )
    valid_until = fields.Method(
        'get_valid_until_string',
        required=True,
        data_key='validUntil',
        type='string',
        format='date-time',
        description='The moment at which the reservation will expire.',
    )

    def get_valid_until_string(self, obj) -> str:
        calc_reservation_deadline = self.context['calc_reservation_deadline']
        return calc_reservation_deadline(obj.created_at).isoformat()


class CreditorActivationRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing=type_registry.creditor_activation_request,
        load_only=True,
        description=TYPE_DESCRIPTION,
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
        description=TYPE_DESCRIPTION,
        example='CreditorDeactivationRequest',
    )
