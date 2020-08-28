from __future__ import annotations
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, true, false, or_
from swpt_creditors.extensions import db
from .common import get_now_utc


# TODO: Implement a daemon that periodically scan the
#       `CommittedTransfer` table and deletes old records (ones having
#       an old `committed_at_ts`). We need to do this to free up disk
#       space.
class CommittedTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    transfer_number = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String, nullable=False)
    sender_id = db.Column(db.String, nullable=False)
    recipient_id = db.Column(db.String, nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_number = db.Column(db.BigInteger, nullable=False)
    __mapper_args__ = {
        'primary_key': [creditor_id, debtor_id, creation_date, transfer_number],
    }
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_data.creditor_id', 'account_data.debtor_id'],
            ondelete='CASCADE',
        ),
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

    account_data = db.relationship('AccountData')


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
            'comment': "Represents a very high probability that there is at least one record in "
                       "the `committed_transfer` table, which should be added to the creditor's "
                       "account ledger.",
        }
    )

    account_data = db.relationship('AccountData')


class DirectTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    debtor_uri = db.Column(
        db.String,
        nullable=False,
        comment="The debtor's URI.",
    )
    recipient_uri = db.Column(
        db.String,
        nullable=False,
        comment="The recipient's URI.",
    )
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_note = db.Column(
        pg.JSON,
        nullable=False,
        default={},
        comment='A note from the sender. Can be any JSON object that the sender wants the '
                'recipient to see.',
    )
    initiated_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the transfer was initiated.',
    )
    finalized_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the transfer was finalized. A `null` means that the '
                'transfer has not been finalized yet.',
    )
    is_successful = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the transfer has been successful or not.',
    )
    json_error = db.Column(
        pg.JSON,
        comment='Describes the reason of the failure, in case the transfer has not been successful.',
    )
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(or_(is_successful == false(), finalized_at_ts != null())),
        db.CheckConstraint(or_(finalized_at_ts == null(), is_successful == true(), json_error != null())),
        {
            'comment': 'Represents an initiated direct transfer. A new row is inserted when '
                       'a creditor creates a new direct transfer. The row is deleted when the '
                       'creditor acknowledges (purges) the transfer.',
        }
    )

    creditor = db.relationship(
        'Creditor',
        backref=db.backref('direct_transfers', cascade="all, delete-orphan", passive_deletes=True),
    )

    @property
    def is_finalized(self):
        return bool(self.finalized_at_ts)

    @property
    def error(self):
        if self.is_finalized and not self.is_successful:
            return self.json_error
        return None  # TODO: is this correct?


class RunningTransfer(db.Model):
    _dcr_seq = db.Sequence('direct_coordinator_request_id_seq', metadata=db.Model.metadata)

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    debtor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The debtor through which the transfer should go.',
    )
    recipient = db.Column(
        db.String,
        nullable=False,
        comment='The recipient of the transfer.',
    )
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_note = db.Column(
        pg.JSON,
        nullable=False,
        comment='A note from the debtor. Can be any JSON object that the debtor wants the recipient '
                'to see.',
    )
    started_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the transfer was started.',
    )
    direct_coordinator_request_id = db.Column(
        db.BigInteger,
        nullable=False,
        server_default=_dcr_seq.next_value(),
        comment='This is the value of the `coordinator_request_id` parameter, which has been '
                'sent with the `prepare_transfer` message for the transfer. The value of '
                '`creditor_id` is sent as the `coordinator_id` parameter. `coordinator_type` '
                'is "direct".',
    )
    direct_transfer_id = db.Column(
        db.BigInteger,
        comment="This value, along with `debtor_id` and `creditor_id` uniquely identifies the "
                "successfully prepared transfer.",
    )
    __mapper_args__ = {'eager_defaults': True}
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        db.Index('idx_direct_coordinator_request_id', creditor_id, direct_coordinator_request_id, unique=True),
        {
            'comment': 'Represents a running direct transfer. Important note: The records for the '
                       'successfully finalized direct transfers (those for which `direct_transfer_id` '
                       'is not `null`), must not be deleted right away. Instead, after they have been '
                       'finalized, they should stay in the database for at least few days. This is '
                       'necessary in order to prevent problems caused by message re-delivery.',
        }
    )

    @property
    def is_finalized(self):
        return self.direct_transfer_id is not None
