from __future__ import annotations
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, or_
from swpt_creditors.extensions import db
from .common import get_now_utc, MAX_INT64

DEFAULT_CREDITOR_STATUS = 0


# TODO: Consider using a `CreditorSpace` model, which may contain a
#       `mask` field. The idea is to know the interval of creditor IDs
#       for which the instance is responsible for, and therefore, not
#       messing up with accounts belonging to other instances.


class Creditor(db.Model):
    STATUS_IS_ACTIVE_FLAG = 1

    creditor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    created_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    status = db.Column(db.SmallInteger, nullable=False, default=DEFAULT_CREDITOR_STATUS)
    last_log_entry_id = db.Column(db.BigInteger, nullable=False, default=0)
    creditor_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    creditor_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    account_list_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    account_list_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    transfer_list_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    transfer_list_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    deactivated_at_date = db.Column(
        db.DATE,
        comment='The date on which the creditor was deactivated. When a creditor gets '
                'deactivated, all its belonging objects (account, transfers, etc.) are '
                'removed. A `NULL` value for this column means that the creditor has '
                'not been deactivated yet. Once deactivated, a creditor stays deactivated '
                'until it is deleted.',
    )
    __table_args__ = (
        db.CheckConstraint(last_log_entry_id >= 0),
        db.CheckConstraint(creditor_latest_update_id > 0),
        db.CheckConstraint(account_list_latest_update_id > 0),
        db.CheckConstraint(transfer_list_latest_update_id > 0),
        db.CheckConstraint(or_(deactivated_at_date == null(), status.op('&')(STATUS_IS_ACTIVE_FLAG) != 0)),
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
        self.last_log_entry_id += 1
        assert 1 <= self.last_log_entry_id <= MAX_INT64
        return self.last_log_entry_id


class BaseLogEntry(db.Model):
    __abstract__ = True

    added_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    object_type = db.Column(db.String, nullable=False)
    object_uri = db.Column(db.String, nullable=False)
    object_update_id = db.Column(db.BigInteger)
    is_deleted = db.Column(db.BOOLEAN, nullable=False, default=False)
    data = db.Column(pg.JSON)

    @property
    def is_created(self):
        return not self.is_deleted and self.object_update_id in [1, None]


class PendingLogEntry(BaseLogEntry):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    pending_entry_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint('object_update_id > 0'),
        {
            'comment': 'Represents a log entry that should be added to the log. Log entries '
                       'are queued to this table because this allows multiple log entries '
                       'for one creditor to be added to the log in one database transaction, '
                       'thus reducing the lock contention on `creditor` table rows.',
        }
    )


class LogEntry(BaseLogEntry):
    creditor_id = db.Column(db.BigInteger, nullable=False)
    entry_id = db.Column(db.BigInteger, nullable=False)
    __mapper_args__ = {
        'primary_key': [creditor_id, entry_id],
    }
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint('object_update_id > 0'),
        db.CheckConstraint(entry_id > 0),

        # TODO: The rest of the columns are not be part of the primary
        #       key, but should be included in the primary key index
        #       to allow index-only scans. Because SQLAlchemy does not
        #       support this yet (2020-01-11), temporarily, there are
        #       no index-only scans.
        db.Index('idx_log_entry_pk', creditor_id, entry_id, unique=True),
    )
