from __future__ import annotations
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, or_
from swpt_creditors.extensions import db
from .common import get_now_utc

SC_OK = 'OK'
SC_UNEXPECTED_ERROR = 'UNEXPECTED_ERROR'
SC_INSUFFICIENT_AVAILABLE_AMOUNT = 'INSUFFICIENT_AVAILABLE_AMOUNT'
SC_CANCELED_BY_THE_SENDER = 'CANCELED_BY_THE_SENDER'


class CommittedTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    transfer_number = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String, nullable=False)
    sender_id = db.Column(db.String, nullable=False)
    recipient_id = db.Column(db.String, nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_number = db.Column(db.BigInteger, nullable=False)
    __mapper_args__ = {
        'primary_key': [creditor_id, debtor_id, creation_date, transfer_number],
    }
    __table_args__ = (
        db.CheckConstraint(transfer_number > 0),
        db.CheckConstraint(acquired_amount != 0),
        db.CheckConstraint(previous_transfer_number >= 0),
        db.CheckConstraint(previous_transfer_number < transfer_number),

        # TODO: `acquired_amount`, `principal`, `committed_at_ts`, and
        #       `previous_transfer_number` columns are not be part of
        #       the primary key, but should be included in the primary
        #       key index to allow index-only scans. Because
        #       SQLAlchemy does not support this yet (2020-01-11),
        #       temporarily, there are no index-only scans.
        db.Index('idx_committed_transfer_pk', creditor_id, debtor_id, creation_date, transfer_number, unique=True),
    )


class PendingLedgerUpdate(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_data.creditor_id', 'account_data.debtor_id'],
            ondelete='CASCADE',
        ),
        {
            'comment': "Represents a good change that there is at least one ledger entry that "
                       "should be added to the creditor's account ledger.",
        }
    )

    account_data = db.relationship('AccountData')


class RunningTransfer(db.Model):
    _cr_seq = db.Sequence('coordinator_request_id_seq', metadata=db.Model.metadata)

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    recipient_uri = db.Column(db.String, nullable=False)
    recipient_id = db.Column(db.String, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)
    initiated_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    finalized_at_ts = db.Column(db.TIMESTAMP(timezone=True))
    error_code = db.Column(db.String)
    total_locked_amount = db.Column(db.BigInteger)
    deadline = db.Column(db.TIMESTAMP(timezone=True))
    min_interest_rate = db.Column(db.REAL, nullable=False, default=-100.0)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False, server_default=_cr_seq.next_value())
    transfer_id = db.Column(db.BigInteger)
    latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __mapper_args__ = {'eager_defaults': True}
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint(amount >= 0),
        db.CheckConstraint(total_locked_amount >= 0),
        db.CheckConstraint(min_interest_rate >= -100.0),
        db.CheckConstraint(locked_amount >= 0),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(or_(error_code == null(), finalized_at_ts != null())),
        db.Index('idx_coordinator_request_id', creditor_id, coordinator_request_id, unique=True),
        {
            'comment': 'Represents an initiated direct transfer. A new row is inserted when '
                       'a creditor initiates a new direct transfer. The row is deleted when the '
                       'creditor deletes the initiated transfer.',
        }
    )

    @property
    def is_finalized(self):
        return bool(self.finalized_at_ts)

    @property
    def is_settled(self):
        return self.transfer_id is not None
