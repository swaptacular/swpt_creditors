from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone, date
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, true, false, or_
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


def date_to_int24(date: date) -> int:
    # TODO: Move this logic to `swpt_lib`.

    days = (date - DATE_2020_01_01).days
    assert days >= 0
    assert days >> 24 == 0
    return days


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

    debtor = db.relationship(
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
        nullable=False,
        default={},
        comment='Notes from the sender. Can be any object that the sender wants the recipient to see.',
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


class CommittedTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(
        db.BigInteger,
        primary_key=True,
        comment="Along with `creditor_id` and `debtor_id` uniquely identifies the committed "
                "transfer. It gets incremented on each committed transfer. Initially, "
                "`transfer_seqnum` has its lowest 40 bits set to zero, and its highest 24 "
                "bits calculated from the value of `transfer_epoch`.",
    )
    transfer_epoch = db.Column(
        db.DATE,
        nullable=False,
        comment="The date on which the account was created. This is needed to detect when "
                "an account has been deleted, and re-created again. (In that case the sequence "
                "of `transfer_seqnum`s will be broken, the old ledger should be discarded, and "
                "a brand new ledger created).",
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
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSON, nullable=False)
    new_account_principal = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The balance on the account after the transfer.',
    )
    __table_args__ = (
        db.CheckConstraint(transfer_seqnum > 0),
        db.CheckConstraint(committed_amount != 0),
        db.CheckConstraint(new_account_principal > MIN_INT64),
        {
            'comment': 'Represents a committed transfer. A new row is inserted when a '
                       '`CommittedTransferSignal` is received. The row is deleted when '
                       'some time (few months for example) has passed.',
        }
    )


class PendingCommittedTransfer(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)

    # TODO: Normally, this column is not part of the primary key, but
    #       because we want it to be included in the index to allow
    #       index-only scans, and SQLAlchemy does not support that yet
    #       (2020-01-11), we include it in the primary key as a
    #       temporary workaround.
    new_account_principal = db.Column(db.BigInteger, primary_key=True)

    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id', 'transfer_seqnum'],
            ['committed_transfer.creditor_id', 'committed_transfer.debtor_id', 'committed_transfer.transfer_seqnum'],
            ondelete='CASCADE',
        ),
        {
            'comment': 'Represents a committed transfer that has not been included in the account '
                       'ledger yet. A new row is inserted when a `CommittedTransferSignal` is received. '
                       'Periodically, the pending rows are processed, added to account ledgers, and then '
                       'deleted. This intermediate storage is necessary, because committed transfers can '
                       'be received out of order, but must be added to the ledgers in order.',
        }
    )


class LedgerAddition(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    addition_seqnum = db.Column(
        db.BigInteger,
        primary_key=True,
        autoincrement=True,
        comment='Determines the order of incoming transfers for each creditor.',
    )
    debtor_id = db.Column(db.BigInteger, nullable=False)
    transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    added_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        db.CheckConstraint(addition_seqnum > 0),
        {
            'comment': "Represents an addition to creditors' account ledgers. This table is needed "
                       "to allow users to store the sequential number of the last seen transfer "
                       "(`addition_seqnum`), and later on, ask only for transfers with bigger "
                       "sequential numbers.",
        }
    )


class AccountLedger(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    epoch = db.Column(db.DATE, nullable=False, default=DATE_2020_01_01)
    principal = db.Column(db.BigInteger, nullable=False, default=0)
    next_transfer_seqnum = db.Column(db.BigInteger, nullable=False, default=1)
    last_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
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
            'comment': 'Contains status information about the ledger of a given account.',
        }
    )


class CreatedAccount(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        {
            'comment': "Represents a created account from users' perspecive. Note that the account "
                       "may still have no corresponding `Account` record.",
        },
    )


class DeletedAccount(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    deleted_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['created_account.creditor_id', 'created_account.debtor_id'],
            ondelete='CASCADE',
        ),
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_deletion_request.creditor_id', 'account_deletion_request.debtor_id'],
            ondelete='CASCADE',
        ),
        {
            'comment': "Represents a deleted account from users' perspecive. Note that the account "
                       "may still have a corresponding `Account` record.",
        }
    )


class AccountDeletionRequest(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sent_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the last `mark_account_for_deletion` signal was sent.',
    )
    ignore_after_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        comment='The `ignore_after_ts` parameter, sent with the last `mark_account_for_deletion` '
                'signal. Must never decrease.',
    )
    negligible_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The `negligible_amount` parameter, sent with the last '
                '`mark_account_for_deletion` signal.',
    )
    is_canceled = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the request was canceled or not. A `True` also implies that there is '
                'no corresponding `DeletedAccount` record.',
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['created_account.creditor_id', 'created_account.debtor_id'],
            ondelete='CASCADE',
        ),
        {
            'comment': 'Represents a request to delete an account.',
        }
    )


class AccountDeletionFailureSignal(Signal):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    deleted_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    deletion_failed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    details = db.Column(pg.JSON, nullable=False)
