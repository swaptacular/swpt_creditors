from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone, date
from marshmallow import Schema, fields
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, true, false, or_, and_, FunctionElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime
from swpt_lib.utils import date_to_int24
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
ROOT_CREDITOR_ID = 0


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
    #      `ModelClass.signalbus_burst_count = N` in models. Make sure
    #      TTL is set properly for the messages.

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

    inserted_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)

# TODO: Consider using a `CreditorSpace` model, which may contain a
#       `mask` field. The idea is to know the interval of creditor IDs
#       for which the instance is responsible for, and therefore, not
#       messing up with accounts belonging to other instances.


class AccountDisplay(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_name = db.Column(db.String)
    amount_divisor = db.Column(db.FLOAT, nullable=False, default=1.0)
    decimal_places = db.Column(db.Integer, nullable=False, default=0)
    own_unit = db.Column(db.String)
    own_unit_preference = db.Column(db.Integer, nullable=False, default=0)
    hide = db.Column(db.BOOLEAN, nullable=False, default=False)
    peg_exchange_rate = db.Column(db.FLOAT)
    peg_debtor_uri = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    latest_update_id = db.Column(db.BigInteger, nullable=False)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.Index(
            'idx_peg_debtor_id',
            creditor_id,
            peg_debtor_id,
            postgresql_where=peg_debtor_id != null(),
        ),
        db.ForeignKeyConstraint(
            ['creditor_id', 'peg_debtor_id'],
            ['account_display.creditor_id', 'account_display.debtor_id'],
        ),
        db.CheckConstraint(amount_divisor > 0.0),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(peg_exchange_rate >= 0.0),
        db.CheckConstraint(and_(
            decimal_places >= -20,
            decimal_places <= 20,
        )),
        db.CheckConstraint(or_(
            peg_exchange_rate != null(),
            peg_debtor_id == null(),
        )),
        db.CheckConstraint(or_(
            peg_exchange_rate == null(),
            peg_debtor_uri != null(),
        )),
    )


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
    latest_log_entry_id = db.Column(db.BigInteger, nullable=False, default=0)
    direct_transfers_count = db.Column(db.Integer, nullable=False, default=0)
    accounts_count = db.Column(db.Integer, nullable=False, default=0)
    __table_args__ = (
        db.CheckConstraint(latest_log_entry_id >= 0),
        db.CheckConstraint(direct_transfers_count >= 0),
        db.CheckConstraint(accounts_count >= 0),
        {
            'comment': "Represents creditor's principal information.",
        }
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
        db.Index(
            'idx_direct_coordinator_request_id',
            creditor_id,
            direct_coordinator_request_id,
            unique=True,
        ),
        db.CheckConstraint(amount > 0),
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


# TODO: Implement a daemon that periodically scan the `AccountCommit`
#       table and deletes old records (ones having an old
#       `committed_at_ts`). We need to do this to free up disk space.
class AccountCommit(db.Model):
    SYSTEM_FLAG_IS_NEGLIGIBLE = 1

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_number = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    transfer_flags = db.Column(db.Integer, nullable=False)
    account_creation_date = db.Column(db.DATE, nullable=False)
    account_new_principal = db.Column(db.BigInteger, nullable=False)
    sender = db.Column(db.String, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_ledger.creditor_id', 'account_ledger.debtor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(transfer_number > 0),
        db.CheckConstraint(committed_amount != 0),
        db.CheckConstraint(account_new_principal > MIN_INT64),
        {
            'comment': 'Represents an account commit. A new row is inserted when a '
                       '`AccountCommitSignal` is received. The row is deleted when '
                       'some time (few months for example) has passed.',
        }
    )

    account_ledger = db.relationship('AccountLedger')


# TODO: Implement a daemon that periodically scan the
#       `PendingAccountCommit` table, finds staled records (ones
#       having an old `committed_at_ts`), deletes them, and mends the
#       account ledger. When a transfer can not be added to the ledger
#       for a long time, this should mean that a preceding transfer
#       has been lost. This should happen very rarely, but still
#       eventually we must be able to recover from such losses.
class PendingAccountCommit(db.Model):
    # TODO: Add `ReadyAccountCommit` model?

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_number = db.Column(db.BigInteger, primary_key=True)

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
            ['creditor_id', 'debtor_id', 'transfer_number'],
            ['account_commit.creditor_id', 'account_commit.debtor_id', 'account_commit.transfer_number'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(committed_amount != 0),
        {
            'comment': 'Represents an account commit that has not been included in the account ledger '
                       'yet. A new row is inserted when a `AccountCommitSignal` is received. '
                       'Periodically, the pending rows are processed, added to account ledgers, and then '
                       'deleted. This intermediate storage is necessary, because account commits can '
                       'be received out-of-order, but must be added to the ledgers in-order.',
        }
    )

    account_commit = db.relationship('AccountCommit')


class AccountConfig(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    has_account = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether a corresponding `account` record exists.',
    )
    is_effectual = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the last change in the configuration has been successfully applied.',
    )
    last_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The timestamp of the last sent `configure_account` signal. Must never decrease.',
    )
    last_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=0,
        comment='The sequential number of the last sent `configure_account` signal. It is '
                'incremented (with wrapping) on every change. This column, along with the '
                '`last_ts` column, allows to reliably determine the correct order of changes, '
                'even if they occur in a very short period of time.',
    )
    is_scheduled_for_deletion = db.Column(db.BOOLEAN, nullable=False, default=False)
    negligible_amount = db.Column(db.REAL, nullable=False, default=0.0)

    # TODO: Those does not belong here.
    account_identity = db.Column(
        db.String,
        comment='The value of the `account_identity` field from the first received '
                '`AccountChangeSignal` for the account.',
    )
    allow_unsafe_removal = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment='Whether the owner approved unsafe removal of the account. In extraordinary '
                'circumstances it might be necessary to forcefully remove an account, accepting '
                'the risk of losing the available amount.',
    )

    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_ledger.creditor_id', 'account_ledger.debtor_id'],
            ondelete='RESTRICT',
        ),
        db.CheckConstraint(negligible_amount >= 0.0),
        {
            'comment': "Represents a configured (created) account from users' perspective. Note "
                       "that a freshly inserted `account_config` record will not have a "
                       "corresponding `account` record.",
        },
    )

    account_ledger = db.relationship(
        'AccountLedger',
        backref=db.backref('account_config', uselist=False),
    )


class AccountLedger(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    account_creation_date = db.Column(
        db.DATE,
        nullable=False,
        default=BEGINNING_OF_TIME.date(),
        comment="The date on which the account was created. This is needed to detect when "
                "an account has been deleted, and recreated again. (In that case the sequence "
                "of `transfer_number`s will be broken, the old ledger should be discarded, and "
                "a brand new ledger created).",
    )
    principal = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The account principal, as it is after the last transfer has been added to the ledger.'
    )
    next_transfer_number = db.Column(
        db.BigInteger,
        nullable=False,
        default=1,
        comment="The anticipated `transfer_number` for the next transfer. It gets incremented when a "
                "new transfer is added to the ledger. For a newly created (or purged, and then "
                "recreated) account, the sequential number of the first transfer will have its lower "
                "40 bits set to `0x0000000001`, and its higher 24 bits calculated from the account's "
                "creation date (the number of days since Jan 1st, 1970). Note that when an account "
                "has been removed from the database, and then recreated again, for this account, a "
                "gap will occur in the generated sequence of `transfer_number`s.",
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
            # with `PendingAccountCommit`. Not sure if it is really
            # beneficial in practice.
            'idx_next_transfer_number',
            creditor_id,
            debtor_id,
            next_transfer_number,
        ),
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(next_transfer_number > 0),
        {
            'comment': 'Represents essential information about the ledger of a given account. Logically '
                       'those columns belong to the `account_config` table, but they are isolated '
                       'here mainly for peformace reasons. The thing is that we want really fast '
                       'index-only scans on the `account_config` table. Normally, the account config '
                       'rarely changes (which is good for the index-only scans), while the account '
                       'ledger information may change very frequently.',
        }
    )

    def reset(self, *, account: Account = None, account_creation_date: date = None, current_ts: datetime = None):
        if account:
            assert account_creation_date is None
            assert account.creditor_id == self.creditor_id and account.debtor_id == self.debtor_id
            self.account_creation_date = account.creation_date
            self.principal = account.principal
            self.next_transfer_number = account.last_transfer_number + 1
        else:
            account_creation_date = account_creation_date or BEGINNING_OF_TIME.date()
            self.account_creation_date = account_creation_date
            self.principal = 0
            self.next_transfer_number = (date_to_int24(account_creation_date) << 40) + 1
        self.last_update_ts = current_ts or datetime.now(tz=timezone.utc)


class LedgerEntry(db.Model):
    # TODO: This is totally wrong. Add a global entry seqnum here and
    #       in `Creditor`, which is incremented for each added
    #       `LedgerEntry`. Fix `process_account_transfer_signal()`
    #       accordingly. (Do we need `added_at_ts`?)

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_number = db.Column(db.BigInteger, primary_key=True)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    account_new_principal = db.Column(db.BigInteger, nullable=False)
    added_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, server_default=utcnow())

    __table_args__ = (
        db.Index(
            # Allows index-only scans in chronological order.
            'idx_ledger_entry_added_at_ts',
            creditor_id,
            added_at_ts,
            debtor_id,
            transfer_number,
            committed_amount,
            account_new_principal,
        ),
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_ledger.creditor_id', 'account_ledger.debtor_id'],
            ondelete='CASCADE',
        ),
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id', 'transfer_number'],
            ['account_commit.creditor_id', 'account_commit.debtor_id', 'account_commit.transfer_number'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(committed_amount != 0),
        db.CheckConstraint(account_new_principal > MIN_INT64),
        {
            'comment': "Represents an entry in one of creditor's account ledgers. This table "
                       "allows users to ask only for transfers that have occurred before or "
                       "after a given moment in time.",
        }
    )

    account_commit = db.relationship('AccountCommit')


class Account(db.Model):
    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    STATUS_UNREACHABLE_FLAG = 1 << 0
    STATUS_OVERFLOWN_FLAG = 1 << 1
    STATUS_DELETED_FLAG = 1 << 16
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 1 << 17

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_transfer_number = db.Column(db.BigInteger, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    status_flags = db.Column(db.Integer, nullable=False)
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the last `AccountChangeSignal` has been processed. It is '
                'used to detect "dead" accounts. A "dead" account is an account that have been '
                'removed from the `swpt_accounts` service, but still exist in this table.',
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_config.creditor_id', 'account_config.debtor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint((interest_rate >= INTEREST_RATE_FLOOR) & (interest_rate <= INTEREST_RATE_CEIL)),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(negligible_amount >= 0.0),
        {
            'comment': 'Tells who owes what to whom. This table is a replica of the table with the '
                       'same name in the `swpt_accounts` service. It is used to perform maintenance '
                       'routines like changing interest rates. Most of the columns get their values '
                       'from the corresponding fields in the last applied `AccountChangeSignal`.',
        }
    )

    account_config = db.relationship(
        'AccountConfig',
        backref=db.backref('account', uselist=False),
    )

    @property
    def is_scheduled_for_deletion(self):
        return bool(self.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG)

    def check_if_config_is_effectual(self):
        config = self.account_config
        return (
            config.is_scheduled_for_deletion == self.is_scheduled_for_deletion
            and config.negligible_amount == self.negligible_amount
        )


class ConfigureAccountSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'configure_account'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Constant(ROOT_CREDITOR_ID)
        ts = fields.DateTime()
        seqnum = fields.Constant(0)
        negligible_amount = fields.Boolean()
        config_flags = fields.Method('get_config_flags')
        config = fields.Constant('')

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    seqnum = db.Column(db.Integer, primary_key=True)
    negligible_amount = db.Column(db.REAL, nullable=False)
    is_scheduled_for_deletion = db.Column(db.BOOLEAN, nullable=False)

    def get_config_flags(self, obj):
        if self.is_scheduled_for_deletion:
            return Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        return 0
