from __future__ import annotations
import math
from datetime import datetime, timezone
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func, null, or_, and_
from swpt_creditors.extensions import db
from .common import (
    get_now_utc,
    ChooseRowsMixin,
    MAX_INT64,
    MIN_INT64,
    TS0,
    DATE0,
    SECONDS_IN_YEAR,
)

HUGE_NEGLIGIBLE_AMOUNT = 1e30
DEFAULT_NEGLIGIBLE_AMOUNT = HUGE_NEGLIGIBLE_AMOUNT
DEFAULT_CONFIG_FLAGS = 0


# NOTE: This sequence is used to generate initial `objectUpdateId`s
# for accounts and account-related objects. To guarantee monotonic
# increase, this sequence must be incremented each time the update ID
# of at least one account-object gets incremented. Note that if the
# update IDs of multiple objects get incremented in one trasaction,
# this sequence can be incremented only once.
uid_seq = db.Sequence("object_update_id_seq", metadata=db.Model.metadata)


class Account(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    created_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["creditor_id"], ["creditor.creditor_id"], ondelete="CASCADE"
        ),
        db.CheckConstraint(latest_update_id > 0),
    )

    data = db.relationship(
        "AccountData", uselist=False, cascade="all", passive_deletes=True
    )
    knowledge = db.relationship(
        "AccountKnowledge", uselist=False, cascade="all", passive_deletes=True
    )
    exchange = db.relationship(
        "AccountExchange", uselist=False, cascade="all", passive_deletes=True
    )
    display = db.relationship(
        "AccountDisplay", uselist=False, cascade="all", passive_deletes=True
    )


class AccountData(db.Model, ChooseRowsMixin):
    STATUS_UNREACHABLE_FLAG = 1 << 0
    STATUS_OVERFLOWN_FLAG = 1 << 1

    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, nullable=False, default=DATE0)
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    last_change_seqnum = db.Column(db.Integer, nullable=False, default=0)
    principal = db.Column(db.BigInteger, nullable=False, default=0)
    interest = db.Column(db.FLOAT, nullable=False, default=0.0)
    last_transfer_number = db.Column(db.BigInteger, nullable=False, default=0)
    last_transfer_committed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )

    # `AccountConfig` data
    last_config_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    last_config_seqnum = db.Column(db.Integer, nullable=False, default=0)
    negligible_amount = db.Column(
        db.REAL, nullable=False, default=DEFAULT_NEGLIGIBLE_AMOUNT
    )
    config_flags = db.Column(
        db.Integer, nullable=False, default=DEFAULT_CONFIG_FLAGS
    )
    config_data = db.Column(db.String, nullable=False, default="")
    is_config_effectual = db.Column(db.BOOLEAN, nullable=False, default=False)
    allow_unsafe_deletion = db.Column(
        db.BOOLEAN, nullable=False, default=False
    )
    has_server_account = db.Column(db.BOOLEAN, nullable=False, default=False)
    config_error = db.Column(db.String)
    config_latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    config_latest_update_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )

    # `AccountInfo` data
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    last_interest_rate_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    transfer_note_max_bytes = db.Column(db.Integer, nullable=False, default=0)
    account_id = db.Column(db.String, nullable=False, default="")
    debtor_info_iri = db.Column(db.String)
    debtor_info_content_type = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)
    info_latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    info_latest_update_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )

    # `AccountLedger` data
    ledger_principal = db.Column(db.BigInteger, nullable=False, default=0)
    ledger_last_entry_id = db.Column(db.BigInteger, nullable=False, default=0)
    ledger_last_transfer_number = db.Column(
        db.BigInteger, nullable=False, default=0
    )
    ledger_pending_transfer_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment=(
            "When there is a committed transfer that can not be added to the"
            " ledger, because a preceding transfer has not been received yet,"
            " this column will contain the the `committed_at` field of the"
            " pending committed transfer. This column is used to identify"
            ' "broken" ledgers that can be "repaired". A NULL means that the'
            " account has no pending committed transfers which the system"
            " knows of."
        ),
    )
    ledger_latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    ledger_latest_update_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["creditor_id", "debtor_id"],
            ["account.creditor_id", "account.debtor_id"],
            ondelete="CASCADE",
        ),
        db.CheckConstraint(interest_rate >= -100.0),
        db.CheckConstraint(transfer_note_max_bytes >= 0),
        db.CheckConstraint(negligible_amount >= 0.0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(ledger_last_entry_id >= 0),
        db.CheckConstraint(ledger_last_transfer_number >= 0),
        db.CheckConstraint(ledger_latest_update_id > 0),
        db.CheckConstraint(config_latest_update_id > 0),
        db.CheckConstraint(info_latest_update_id > 0),
        db.CheckConstraint(
            or_(
                debtor_info_sha256 == null(),
                func.octet_length(debtor_info_sha256) == 32,
            )
        ),
    )

    @property
    def is_scheduled_for_deletion(self):
        return bool(
            self.config_flags & self.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        )

    @is_scheduled_for_deletion.setter
    def is_scheduled_for_deletion(self, value):
        if value:
            self.config_flags |= self.CONFIG_SCHEDULED_FOR_DELETION_FLAG
        else:
            self.config_flags &= ~self.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    @property
    def is_deletion_safe(self):
        return (
            not self.has_server_account
            and self.is_scheduled_for_deletion
            and self.is_config_effectual
        )

    @property
    def ledger_interest(self) -> int:
        interest = self.interest
        current_balance = self.principal + interest
        if current_balance > 0.0:
            current_ts = datetime.now(tz=timezone.utc)
            passed_seconds = max(
                0.0, (current_ts - self.last_change_ts).total_seconds()
            )
            try:
                k = (
                    math.log(1.0 + self.interest_rate / 100.0)
                    / SECONDS_IN_YEAR
                )
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

        return interest


class AccountKnowledge(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    data = db.Column(pg.JSON, nullable=False, default={})
    latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["creditor_id", "debtor_id"],
            ["account.creditor_id", "account.debtor_id"],
            ondelete="CASCADE",
        ),
        db.CheckConstraint(latest_update_id > 0),
    )


class AccountExchange(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    policy = db.Column(db.String)
    min_principal = db.Column(db.BigInteger, nullable=False, default=MIN_INT64)
    max_principal = db.Column(db.BigInteger, nullable=False, default=MAX_INT64)
    peg_exchange_rate = db.Column(db.FLOAT)
    peg_debtor_id = db.Column(db.BigInteger)
    latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["creditor_id", "debtor_id"],
            ["account.creditor_id", "account.debtor_id"],
            ondelete="CASCADE",
        ),
        db.ForeignKeyConstraint(
            ["creditor_id", "peg_debtor_id"],
            ["account_exchange.creditor_id", "account_exchange.debtor_id"],
            deferrable=True,
            initially="DEFERRED",
        ),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(min_principal <= max_principal),
        db.CheckConstraint(peg_exchange_rate >= 0.0),
        db.CheckConstraint(
            or_(
                and_(peg_debtor_id == null(), peg_exchange_rate == null()),
                and_(peg_debtor_id != null(), peg_exchange_rate != null()),
            )
        ),
        db.Index(
            "idx_peg_debtor_id",
            creditor_id,
            peg_debtor_id,
            postgresql_where=peg_debtor_id != null(),
        ),
    )


class AccountDisplay(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_name = db.Column(db.String)
    amount_divisor = db.Column(db.FLOAT, nullable=False, default=1.0)
    decimal_places = db.Column(db.Integer, nullable=False, default=0)
    unit = db.Column(db.String)
    known_debtor = db.Column(db.BOOLEAN, nullable=False, default=False)
    latest_update_id = db.Column(
        db.BigInteger, nullable=False, server_default=uid_seq.next_value()
    )
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["creditor_id", "debtor_id"],
            ["account.creditor_id", "account.debtor_id"],
            ondelete="CASCADE",
        ),
        db.CheckConstraint(amount_divisor > 0.0),
        db.CheckConstraint(latest_update_id > 0),
        db.Index(
            "idx_debtor_name",
            creditor_id,
            debtor_name,
            unique=True,
            postgresql_where=debtor_name != null(),
        ),
    )


class LedgerEntry(db.Model, ChooseRowsMixin):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    entry_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The rest of the columns are not be part of the primary
    # key, but should be included in the primary key index to allow
    # index-only scans. Because SQLAlchemy does not support this yet
    # (2020-01-11), the migration file should be edited so as not to
    # create a "normal" index, but create a "covering" index instead.
    creation_date = db.Column(db.DATE)
    transfer_number = db.Column(db.BigInteger)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    added_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(transfer_number > 0),
        db.CheckConstraint(entry_id > 0),
        db.CheckConstraint(
            or_(
                and_(creation_date == null(), transfer_number == null()),
                and_(creation_date != null(), transfer_number != null()),
            )
        ),
    )
