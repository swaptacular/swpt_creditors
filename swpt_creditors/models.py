from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone, date
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, true, false, or_, FunctionElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
DATE_2020_01_01 = date(2020, 1, 1)
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
ROOT_CREDITOR_ID = 0


def increment_seqnum(n):  # pragma: no cover
    return MIN_INT32 if n == MAX_INT32 else n + 1


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class utcnow(FunctionElement):
    type = DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models.

    queue_name: Optional[str] = None

    @property
    def event_name(self):  # pragma: no cover
        model = type(self)
        return f'on_{model.__tablename__}'

    def send_signalbus_message(self):  # pragma: no cover
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.actor_name is set, but SignalModel.queue_name is not'
            actor_name = self.event_name
            routing_key = f'events.{actor_name}'
        else:
            actor_name = model.actor_name
            routing_key = model.queue_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        broker.publish_message(message, exchange=MAIN_EXCHANGE_NAME, routing_key=routing_key)


class Creditor(db.Model):
    STATUS_IS_ACTIVE_FLAG = 1

    creditor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=f"Creditor's status bits: {STATUS_IS_ACTIVE_FLAG} - is active.",
    )
    created_at_date = db.Column(
        db.DATE,
        nullable=False,
        default=get_now_utc,
        comment='The date on which the creditor was created.',
    )
    deactivated_at_date = db.Column(
        db.DATE,
        comment='The date on which the creditor was deactivated. A `null` means that the '
                'creditor has not been deactivated yet. Management operations (like making '
                'direct transfers) are not allowed on deactivated creditors. Once '
                'deactivated, a creditor stays deactivated until it is deleted. Important '
                'note: All creditors are created with their "is active" status bit set to `0`, '
                'and it gets set to `1` only after the first management operation has been '
                'performed.',
    )

    @property
    def is_active(self):
        return bool(self.status & Creditor.STATUS_IS_ACTIVE_FLAG)

    @is_active.setter
    def is_active(self, value):
        if value:
            self.status |= Creditor.STATUS_IS_ACTIVE_FLAG
        else:
            self.status &= ~Creditor.STATUS_IS_ACTIVE_FLAG


class InitiatedTransfer(db.Model):
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
    transfer_info = db.Column(
        pg.JSON,
        nullable=False,
        default={},
        comment='Notes from the sender. Can be any object that the sender wants the recipient to see.',
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
    error_code = db.Column(
        db.String,
        comment="The error code, in case the transfer has not been successful.",
    )
    error_message = db.Column(
        db.String,
        comment="The error message, in case the transfer has not been successful.",
    )
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(or_(is_successful == false(), finalized_at_ts != null())),
        db.CheckConstraint(or_(finalized_at_ts == null(), is_successful == true(), error_code != null())),
        db.CheckConstraint(or_(error_code == null(), error_message != null())),
        {
            'comment': 'Represents an initiated direct transfer. A new row is inserted when '
                       'a creditor creates a new direct transfer. The row is deleted when the '
                       'creditor acknowledges (purges) the transfer.',
        }
    )

    creditor = db.relationship(
        'Creditor',
        backref=db.backref('initiated_transfers', cascade="all, delete-orphan", passive_deletes=True),
    )

    @property
    def is_finalized(self):
        return bool(self.finalized_at_ts)

    @property
    def errors(self):
        if self.is_finalized and not self.is_successful:
            return [{'error_code': self.error_code, 'message': self.error_message}]
        return []


class RunningTransfer(db.Model):
    _dcr_seq = db.Sequence('direct_coordinator_request_id_seq', metadata=db.Model.metadata)

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True), primary_key=True)
    debtor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The debtor through which the transfer should go.',
    )
    recipient_creditor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The recipient of the transfer.',
    )
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The amount to be transferred. Must be positive.',
    )
    transfer_info = db.Column(
        pg.JSON,
        comment='Notes from the sender. Can be any JSON object that the sender wants the recipient '
                'to see. Can be set `null` (to save disk space) only after the transfer has been '
                'finalized.',
    )
    finalized_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the transfer was finalized. A `null` means that the '
                'transfer has not been finalized yet.',
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
        db.Index(
            'idx_direct_coordinator_request_id',
            debtor_id,
            direct_coordinator_request_id,
            unique=True,
        ),
        db.CheckConstraint(or_(direct_transfer_id == null(), finalized_at_ts != null())),
        db.CheckConstraint(or_(transfer_info != null(), finalized_at_ts != null())),
        db.CheckConstraint(amount > 0),
        {
            'comment': 'Represents a running direct transfer. Important note: The records for the '
                       'finalized direct transfers (failed or successful) must not be deleted '
                       'right away. Instead, after they have been finalized, they should stay in '
                       'the database for at least few days. This is necessary in order to prevent '
                       'problems caused by message re-delivery.',
        }
    )

    @property
    def is_finalized(self):
        return bool(self.finalized_at_ts)


# TODO: Implement a daemon that periodically scan the
#       `CommittedTransfer` table and deletes old records (ones having
#       an old `committed_at_ts`). We need to do this to free up disk
#       space.
class CommittedTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(
        db.BigInteger,
        primary_key=True,
        comment="Along with `creditor_id` and `debtor_id` uniquely identifies the committed "
                "transfer. It gets incremented on each committed transfer. Initially, "
                "`transfer_seqnum` has its lowest 40 bits set to zero, and its highest 24 "
                "bits calculated from the value of `account_creation_date`.",
    )
    coordinator_type = db.Column(
        db.String(30),
        nullable=False,
        comment='Indicates which subsystem has committed the transfer.',
    )
    other_creditor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The creditor ID of other party in the transfer. When `committed_amount` is '
                'positive, this is the sender. When `committed_amount` is negative, this is '
                'the recipient.',
    )
    committed_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='The moment at which the transfer was committed.',
    )
    committed_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment="This is the change in the account's principal that the transfer caused. Can "
                "be positive or negative. Can not be zero.",
    )
    transfer_info = db.Column(pg.JSON, nullable=False)
    account_creation_date = db.Column(
        db.DATE,
        nullable=False,
        comment="The date on which the account was created. This is needed to detect when "
                "an account has been deleted, and re-created again. (In that case the sequence "
                "of `transfer_seqnum`s will be broken, the old ledger should be discarded, and "
                "a brand new ledger created).",
    )
    account_new_principal = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The balance on the account after the transfer.',
    )
    __table_args__ = (
        db.CheckConstraint(transfer_seqnum > 0),
        db.CheckConstraint(committed_amount != 0),
        db.CheckConstraint(account_new_principal > MIN_INT64),
        {
            'comment': 'Represents a committed transfer. A new row is inserted when a '
                       '`CommittedTransferSignal` is received. The row is deleted when '
                       'some time (few months for example) has passed.',
        }
    )


# TODO: Implement a daemon that periodically scan the
#       `PendingCommittedTransfer` table, finds staled records (ones
#       having an old `committed_at_ts`), deletes them, and mends the
#       account ledger. When a transfer can not be added to the ledger
#       for a long time, this should mean that a preceding transfer
#       has been lost. This should happen very rarely, but still
#       eventually we must be able to recover from such losses.
class PendingCommittedTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)

    # TODO: Normally, these columns are not part of the primary key,
    #       but because we want them to be included in the index to
    #       allow index-only scans, and SQLAlchemy does not support
    #       that yet (2020-01-11), we include them in the primary key
    #       as a temporary workaround.
    committed_amount = db.Column(db.BigInteger, primary_key=True)
    account_new_principal = db.Column(db.BigInteger, primary_key=True)

    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id', 'transfer_seqnum'],
            ['committed_transfer.creditor_id', 'committed_transfer.debtor_id', 'committed_transfer.transfer_seqnum'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(committed_amount != 0),
        {
            'comment': 'Represents a committed transfer that has not been included in the account '
                       'ledger yet. A new row is inserted when a `CommittedTransferSignal` is received. '
                       'Periodically, the pending rows are processed, added to account ledgers, and then '
                       'deleted. This intermediate storage is necessary, because committed transfers can '
                       'be received out of order, but must be added to the ledgers in order.',
        }
    )

    committed_transfer = db.relationship('CommittedTransfer')


class AccountConfig(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    is_effectual = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the last change in the configuration has been successfully applied.'
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='The timestamp of the last change in the configuration. Must never decrease.',
    )
    last_change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        comment='The sequential number of the last change in the configuration. It is incremented '
                '(with wrapping) on every change. This column, along with the `last_change_ts` '
                'column, allows to reliably determine the correct order of changes, even if '
                'they occur in a very short period of time.',
    )
    is_scheduled_for_deletion = db.Column(
        db.BOOLEAN,
        nullable=False,
        comment='The maximum account balance that should be considered negligible. It is used to '
                'decide whether an account can be safely deleted.',
    )
    negligible_amount = db.Column(
        db.REAL,
        nullable=False,
        comment='Whether the account is scheduled for deletion.',
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_ledger.creditor_id', 'account_ledger.debtor_id'],
            ondelete='RESTRICT',
        ),
        db.CheckConstraint(negligible_amount >= 2.0),
        {
            'comment': "Represents a configured (created) account from users' perspective. Note "
                       "that a freshly inserted `account_config` record will have no corresponding "
                       "`account` record. Also, an `account_config` record must not be deleted, "
                       "unless its `is_effectual` column is `true` and there is no corresponding "
                       "`account` record.",
        },
    )

    account_ledger = db.relationship('AccountLedger')


class AccountIssue(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    issue_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    issue_type = db.Column(db.String(30), nullable=False)
    issue_can_be_discarded = db.Column(db.BOOLEAN, nullable=False)
    issue_raised_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    details = db.Column(pg.JSON, nullable=False, default={})
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_config.creditor_id', 'account_config.debtor_id'],
            ondelete='CASCADE',
        ),
        db.Index('idx_account_issue_debtor_id', creditor_id, debtor_id),
        {
            'comment': 'Represents a problem with a given account that needs attention.',
        }
    )

    account_config = db.relationship(
        'AccountConfig',
        backref=db.backref('account_issues', cascade="all, delete-orphan", passive_deletes=True),
    )


class AccountLedger(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    account_creation_date = db.Column(
        db.DATE,
        nullable=False,
        default=DATE_2020_01_01,
        comment='The date at which the account was created. This column allows to correctly recognize '
                'the situation when the account has been purged, and then recreated.'
    )
    principal = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The account principal, as it is after the last transfer has been added to the ledger.'
    )
    next_transfer_seqnum = db.Column(
        db.BigInteger,
        nullable=False,
        default=1,
        comment="The anticipated `transfer_seqnum` for the next transfer. It gets incremented when a "
                "new transfer is added to the ledger. For a newly created (or purged, and then "
                "recreated) account, the sequential number of the first transfer will have its lower "
                "40 bits set to `0x0000000001`, and its higher 24 bits calculated from the account's "
                "creation date (the number of days since Jan 1st, 2020). Note that when an account "
                "has been removed from the database, and then recreated again, for this account, a "
                "gap will occur in the generated sequence of `transfer_seqnum`s.",
    )
    last_update_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the most recent change in the row happened.',
    )
    __table_args__ = (
        db.Index(
            # This index is supposed to allow efficient merge joins
            # with `PendingCommittedTransfer`. Not sure if it is
            # really needed in practice.
            'idx_next_transfer_seqnum',
            creditor_id,
            debtor_id,
            next_transfer_seqnum,
        ),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(next_transfer_seqnum > 0),
        {
            'comment': 'Contains information about the ledger of a given account.',
        }
    )


# TODO: Implement a daemon that periodically scan the `LedgerEntry`
#       table and deletes old records (ones having an old
#       `added_at_ts`).  We need to do this to free up disk space.
class LedgerEntry(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)

    # Semantically, this column is not be part of the primary key, but
    # because we want ledger entries to be ordered chronologically, we
    # include it in the index for performance reasons.
    added_at_ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True, server_default=utcnow())

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)

    # TODO: Normally, these columns also are not part of the primary
    #       key, but we want them to be included in the index to allow
    #       index-only scans, and because SQLAlchemy does not support
    #       that yet (2020-01-11), we include them in the primary key
    #       as a temporary workaround.
    committed_amount = db.Column(db.BigInteger, primary_key=True)
    account_new_principal = db.Column(db.BigInteger, primary_key=True)

    __table_args__ = (
        db.CheckConstraint(committed_amount != 0),
        db.CheckConstraint(account_new_principal > MIN_INT64),
        {
            'comment': "Represents an entry in one of creditor's account ledgers. This table "
                       "allows users to ask only for transfers that have occurred after a given "
                       "moment in time.",
        }
    )
