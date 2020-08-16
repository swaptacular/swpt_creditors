from __future__ import annotations
import math
from typing import Optional, Tuple
from datetime import datetime, timezone, date, timedelta
from marshmallow import Schema, fields
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, true, false, func, or_, and_, FunctionElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime
from swpt_creditors.extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
TS0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
DATE0 = TS0.date()
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
ROOT_CREDITOR_ID = 0
FIRST_LOG_ENTRY_ID = 2
DEFAULT_CONFIG_FLAGS = 0
DEFAULT_NEGLIGIBLE_AMOUNT = 1e30


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class utcnow(FunctionElement):
    type = DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


def make_transfer_slug(creation_date: date, transfer_number: int) -> str:
    epoch = (creation_date - DATE0).days
    return f'{epoch}-{transfer_number}'


def parse_transfer_slug(slug) -> Tuple[date, int]:
    epoch, transfer_number = slug.split('-', maxsplit=1)
    epoch = int(epoch)
    transfer_number = int(transfer_number)

    try:
        creation_date = DATE0 + timedelta(days=epoch)
    except OverflowError:
        raise ValueError from None

    if not 1 <= transfer_number <= MAX_INT64:
        raise ValueError

    assert isinstance(creation_date, date)
    assert isinstance(transfer_number, int)
    return creation_date, transfer_number


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


class Creditor(db.Model):
    STATUS_IS_ACTIVE_FLAG = 1

    creditor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    created_at_date = db.Column(db.DATE, nullable=False, default=get_now_utc)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=f"Creditor's status bits: {STATUS_IS_ACTIVE_FLAG} - is active.",
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
    latest_log_entry_id = db.Column(
        db.BigInteger,
        nullable=False,
        default=1,
        comment='Gets incremented each time a new entry is added to the log.',
    )
    creditor_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    creditor_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    account_list_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    account_list_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    transfer_list_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    transfer_list_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        db.CheckConstraint(latest_log_entry_id > 0),
        db.CheckConstraint(creditor_latest_update_id > 0),
        db.CheckConstraint(account_list_latest_update_id > 0),
        db.CheckConstraint(transfer_list_latest_update_id > 0),
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

    def generate_log_entry_id(self):
        log_entry_id = self.latest_log_entry_id + 1
        assert log_entry_id <= MAX_INT64
        self.latest_log_entry_id = log_entry_id
        return log_entry_id


class BaseLogEntry(db.Model):
    __abstract__ = True

    added_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    object_type = db.Column(db.String, nullable=False)
    object_uri = db.Column(db.String, nullable=False)
    object_update_id = db.Column(db.BigInteger)
    is_deleted = db.Column(db.BOOLEAN, nullable=False, default=False)
    data = db.Column(pg.JSON)


class LogEntry(BaseLogEntry):
    creditor_id = db.Column(db.BigInteger, nullable=False)
    entry_id = db.Column(db.BigInteger, nullable=False)
    previous_entry_id = db.Column(db.BigInteger, nullable=False)
    __mapper_args__ = {
        'primary_key': [creditor_id, entry_id],
    }
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint('object_update_id > 0'),
        db.CheckConstraint(entry_id > 0),
        db.CheckConstraint(and_(previous_entry_id > 0, previous_entry_id < entry_id)),

        # TODO: The rest of the columns are not be part of the primary
        #       key, but should be included in the primary key index
        #       to allow index-only scans. Because SQLAlchemy does not
        #       support this yet (2020-01-11), temporarily, there are
        #       no index-only scans.
        db.Index('idx_log_entry_pk', creditor_id, entry_id, unique=True),
    )


class PendingLogEntry(BaseLogEntry):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    pending_entry_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint('object_update_id > 0'),
    )


class Account(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint(latest_update_id > 0),
    )

    data = db.relationship('AccountData', uselist=False, cascade='all', passive_deletes=True)
    knowledge = db.relationship('AccountKnowledge', uselist=False, cascade='all', passive_deletes=True)
    exchange = db.relationship('AccountExchange', uselist=False, cascade='all', passive_deletes=True)
    display = db.relationship('AccountDisplay', uselist=False, cascade='all', passive_deletes=True)


# TODO: Implement a daemon that periodically scan the `AccountData`
#       table and makes sure that the `config_error` filed is set for
#       each record that has an old `last_config_ts`, and is not
#       effectual (`is_config_effectual is False`).
class AccountData(db.Model):
    STATUS_UNREACHABLE_FLAG = 1 << 0
    STATUS_OVERFLOWN_FLAG = 1 << 1

    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, nullable=False, default=DATE0)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=TS0)
    last_change_seqnum = db.Column(db.Integer, nullable=False, default=0)
    principal = db.Column(db.BigInteger, nullable=False, default=0)
    interest = db.Column(db.FLOAT, nullable=False, default=0.0)
    last_transfer_number = db.Column(db.BigInteger, nullable=False, default=0)
    last_transfer_committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=TS0)
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the last `AccountUpdate` message has been processed. It is '
                'used to detect "dead" accounts. A "dead" account is an account that have been '
                'removed from the `swpt_accounts` service, but still exist in this table.',
    )

    # AccountConfig data
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=TS0)
    last_config_seqnum = db.Column(db.Integer, nullable=False, default=0)
    negligible_amount = db.Column(db.REAL, nullable=False, default=DEFAULT_NEGLIGIBLE_AMOUNT)
    config_flags = db.Column(db.Integer, nullable=False, default=DEFAULT_CONFIG_FLAGS)
    is_config_effectual = db.Column(db.BOOLEAN, nullable=False, default=False)
    allow_unsafe_deletion = db.Column(db.BOOLEAN, nullable=False, default=False)
    has_server_account = db.Column(db.BOOLEAN, nullable=False, default=False)
    config_error = db.Column(db.String)
    config_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    config_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    # AccountInfo data
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    last_interest_rate_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=TS0)
    status_flags = db.Column(db.Integer, nullable=False, default=STATUS_UNREACHABLE_FLAG)
    account_id = db.Column(db.String, nullable=False, default='')
    debtor_info_url = db.Column(db.String)
    info_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    info_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    # AccountLedger data
    ledger_principal = db.Column(db.BigInteger, nullable=False, default=0)
    ledger_last_transfer_number = db.Column(db.BigInteger, nullable=False, default=0)
    ledger_last_transfer_committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=TS0)
    ledger_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    ledger_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    ledger_latest_entry_id = db.Column(db.BigInteger, nullable=False, default=0)

    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account.creditor_id', 'account.debtor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(interest_rate >= -100.0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(negligible_amount >= 0.0),
        db.CheckConstraint(config_latest_update_id > 0),
        db.CheckConstraint(info_latest_update_id > 0),
        db.CheckConstraint(ledger_latest_update_id > 0),
        db.CheckConstraint(ledger_last_transfer_number >= 0),
        db.CheckConstraint(ledger_latest_entry_id >= 0),

        # This index is supposed to allow efficient merge joins with
        # `PendingAccountCommit`. Not sure if it is actually
        # beneficial in practice.
        db.Index('idx_ledger_last_transfer', creditor_id, debtor_id, creation_date, ledger_last_transfer_number),
    )

    @property
    def overflown(self):
        return bool(self.status_flags & self.STATUS_OVERFLOWN_FLAG)

    @property
    def is_scheduled_for_deletion(self):
        return bool(self.config_flags & self.CONFIG_SCHEDULED_FOR_DELETION_FLAG)

    @is_scheduled_for_deletion.setter
    def is_scheduled_for_deletion(self, value):
        if value:
            self.config_flags |= self.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        else:
            self.config_flags &= ~self.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    @property
    def is_deletion_safe(self):
        return not self.has_server_account and self.is_scheduled_for_deletion and self.is_config_effectual

    @property
    def ledger_interest(self):
        interest = self.interest
        current_balance = self.principal + interest
        if current_balance > 0.0:
            current_ts = datetime.now(tz=timezone.utc)
            passed_seconds = max(0.0, (current_ts - self.last_change_ts).total_seconds())
            try:
                k = math.log(1.0 + self.interest_rate / 100.0) / SECONDS_IN_YEAR
                current_balance *= math.exp(k * passed_seconds)
            except ValueError:
                assert self.interest_rate < -99.9999
                current_balance = 0.0
            interest = current_balance - self.principal

        if math.isnan(interest):
            interest = 0.0
        if math.isfinite(interest):
            interest = math.floor(interest)
        if interest > MAX_INT64:
            interest = MAX_INT64
        if interest < MIN_INT64:
            interest = MIN_INT64

        assert isinstance(interest, int)
        assert MIN_INT64 <= interest <= MAX_INT64
        return interest


class AccountKnowledge(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    interest_rate_changed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=TS0)
    account_identity = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)
    latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account.creditor_id', 'account.debtor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(func.octet_length(debtor_info_sha256) == 32),
    )


class AccountExchange(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    policy = db.Column(db.String)
    min_principal = db.Column(db.BigInteger, nullable=False, default=MIN_INT64)
    max_principal = db.Column(db.BigInteger, nullable=False, default=MAX_INT64)
    latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account.creditor_id', 'account.debtor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(min_principal <= max_principal),
    )


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
    peg_currency_debtor_id = db.Column(db.BigInteger)
    peg_account_debtor_id = db.Column(db.BigInteger)
    peg_debtor_home_url = db.Column(db.String)
    latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account.creditor_id', 'account.debtor_id'],
            ondelete='CASCADE',
        ),
        db.ForeignKeyConstraint(
            ['creditor_id', 'peg_account_debtor_id'],
            ['account_display.creditor_id', 'account_display.debtor_id'],
        ),
        db.CheckConstraint(amount_divisor > 0.0),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(peg_exchange_rate >= 0.0),
        db.CheckConstraint(or_(debtor_name != null(), own_unit == null())),
        db.CheckConstraint(or_(debtor_name != null(), peg_exchange_rate == null())),
        db.CheckConstraint(or_(peg_exchange_rate != null(), peg_account_debtor_id == null())),
        db.CheckConstraint(or_(peg_currency_debtor_id != null(), peg_exchange_rate == null())),
        db.CheckConstraint(or_(peg_account_debtor_id == peg_currency_debtor_id, peg_account_debtor_id == null())),
        db.Index('idx_debtor_name', creditor_id, debtor_name, unique=True, postgresql_where=debtor_name != null()),
        db.Index('idx_own_unit', creditor_id, own_unit, unique=True, postgresql_where=own_unit != null()),
        db.Index(
            'idx_account_peg_debtor_id',
            creditor_id,
            peg_account_debtor_id,
            postgresql_where=peg_account_debtor_id != null(),
        ),
        db.Index(
            'idx_peg_currency_debtor_id',
            creditor_id,
            peg_currency_debtor_id,
            postgresql_where=peg_currency_debtor_id != null(),
        ),
    )


class LedgerEntry(db.Model):
    creditor_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    entry_id = db.Column(db.BigInteger, nullable=False)
    creation_date = db.Column(db.DATE)
    transfer_number = db.Column(db.BigInteger)
    aquired_amount = db.Column(db.BigInteger, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    added_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    previous_entry_id = db.Column(db.BigInteger)

    __mapper_args__ = {
        'primary_key': [creditor_id, debtor_id, entry_id],
    }
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['creditor_id', 'debtor_id'],
            ['account_data.creditor_id', 'account_data.debtor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(transfer_number > 0),
        db.CheckConstraint(entry_id > 0),
        db.CheckConstraint(and_(previous_entry_id > 0, previous_entry_id < entry_id)),

        # TODO: The rest of the columns are not be part of the primary
        #       key, but should be included in the primary key index
        #       to allow index-only scans. Because SQLAlchemy does not
        #       support this yet (2020-01-11), temporarily, there are
        #       no index-only scans.
        db.Index('idx_ledger_entry_pk', creditor_id, debtor_id, entry_id, unique=True),
    )


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

        # TODO: `acquired_amount` and `principal` columns are not be
        #       part of the primary key, but should be included in the
        #       primary key index to allow index-only scans. Because
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
            'comment': "Represents a very high probability that there is at least one record in "
                       "the `committed_transfer` table, which should be added to the creditor's "
                       "account ledger.",
        }
    )


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
    creation_date = db.Column(db.DATE, primary_key=True)
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
            ['creditor_id', 'debtor_id', 'creation_date', 'transfer_number'],
            [
                'committed_transfer.creditor_id',
                'committed_transfer.debtor_id',
                'committed_transfer.creation_date',
                'committed_transfer.transfer_number',
            ],
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

    committed_transfer = db.relationship('CommittedTransfer')


class ConfigureAccountSignal(Signal):
    queue_name = 'swpt_accounts'
    actor_name = 'configure_account'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Constant(ROOT_CREDITOR_ID)
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
