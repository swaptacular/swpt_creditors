from __future__ import annotations
from marshmallow import Schema, fields
from swpt_creditors.extensions import db
from .common import Signal


class ConfigureAccountSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'configure_account'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        ts = fields.DateTime()
        seqnum = fields.Constant(0)
        negligible_amount = fields.Float()
        config = fields.String()
        config_flags = fields.Integer()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    seqnum = db.Column(db.Integer, primary_key=True)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config = db.Column(db.String, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.CheckConstraint(negligible_amount >= 0.0),
    )


class PrepareTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'prepare_transfer'

    class __marshmallow__(Schema):
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        coordinator_type = fields.Constant('direct')
        coordinator_id = fields.Integer(attribute='creditor_id', dump_only=True)
        coordinator_request_id = fields.Integer()
        min_locked_amount = fields.Integer(attribute='amount', dump_only=True)
        max_locked_amount = fields.Integer(attribute='amount', dump_only=True)
        recipient = fields.String()
        min_account_balance = fields.Constant(0)
        min_interest_rate = fields.Float()
        max_commit_delay = fields.Integer()
        inserted_at_ts = fields.DateTime(data_key='ts')

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    min_interest_rate = db.Column(db.Float, nullable=False)
    max_commit_delay = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class FinalizeTransferSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'finalize_transfer'

    class __marshmallow__(Schema):
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.Constant('direct')
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        committed_amount = fields.Integer()
        transfer_note = fields.String()
        finalization_flags = fields.Constant(0)
        inserted_at_ts = fields.DateTime(data_key='ts')

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    __table_args__ = (
        db.CheckConstraint(committed_amount >= 0),
    )
