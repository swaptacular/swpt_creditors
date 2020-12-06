from __future__ import annotations
from marshmallow import Schema, fields
from swpt_creditors.extensions import db
from .common import Signal, CT_DIRECT


class ConfigureAccountSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'configure_account'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        ts = fields.DateTime()
        seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    seqnum = db.Column(db.Integer, primary_key=True)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_data = db.Column(db.String, nullable=False, default='')
    config_flags = db.Column(db.Integer, nullable=False)


class PrepareTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'prepare_transfer'

    class __marshmallow__(Schema):
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        coordinator_type = fields.Constant(CT_DIRECT)
        coordinator_id = fields.Integer(attribute='creditor_id', dump_only=True)
        coordinator_request_id = fields.Integer()
        min_locked_amount = fields.Integer(attribute='locked_amount', dump_only=True)
        max_locked_amount = fields.Integer(attribute='locked_amount', dump_only=True)
        recipient = fields.String()
        min_interest_rate = fields.Float()
        max_commit_delay = fields.Integer()
        inserted_at = fields.DateTime(data_key='ts')

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    min_interest_rate = db.Column(db.Float, nullable=False)
    max_commit_delay = db.Column(db.Integer, nullable=False)


class FinalizeTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'finalize_transfer'

    class __marshmallow__(Schema):
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.Constant(CT_DIRECT)
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        committed_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        finalization_flags = fields.Integer()
        inserted_at = fields.DateTime(data_key='ts')

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    finalization_flags = db.Column(db.Integer, nullable=False)
