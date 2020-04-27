from marshmallow import Schema, fields

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
URI_DESCRIPTION = 'The URI of this object. Can be a relative URI.'


class ObjectReferenceSchema(Schema):
    uri = fields.Url(
        required=True,
        format='uri-reference',
        description="The URI of the object. Can be a relative URI.",
        example='https://example.com/objects/1',
    )


class AccountInfoSchema(Schema):
    type = fields.String(
        required=True,
        description="The type of this object. Different debtor types may use different "
                    "schemas for the information about their accounts. The provided "
                    "information must be enough to: 1) uniquely identify the debtor, "
                    "2) uniquely identify the creditor's account with the debtor. This "
                    "field contains the name of the used schema.",
        example='AccountInfo',
    )
