from __future__ import annotations
import logging
import hmac
from math import exp
from typing import Dict, Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func, true, null, or_, and_
from swpt_creditors.extensions import db
from .common import get_now_utc, ROOT_CREDITOR_ID

DEFAULT_CREDITOR_STATUS = 0


class AgentConfig(db.Model):
    is_effective = db.Column(db.BOOLEAN, primary_key=True, default=True)
    min_creditor_id = db.Column(db.BigInteger, nullable=False)
    max_creditor_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(is_effective == true()),
        db.CheckConstraint(min_creditor_id <= max_creditor_id),
        db.CheckConstraint(or_(
            min_creditor_id > ROOT_CREDITOR_ID,
            max_creditor_id < ROOT_CREDITOR_ID,
        )),
        {
            'comment': 'Represents the global agent configuration (a singleton). The '
                       'agent is responsible only for creditor IDs that are within the '
                       'interval [min_creditor_id, max_creditor_id].',
        }
    )


class Creditor(db.Model):
    STATUS_IS_ACTIVATED_FLAG = 1 << 0
    STATUS_IS_DEACTIVATED_FLAG = 1 << 1

    _ac_seq = db.Sequence('creditor_reservation_id_seq', metadata=db.Model.metadata)

    creditor_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The `status_flags` column is not be part of the primary
    # key, but should be included in the primary key index to allow
    # index-only scans. Because SQLAlchemy does not support this yet
    # (2020-01-11), the migration file should be edited so as not to
    # create a "normal" index, but create a "covering" index instead.
    status_flags = db.Column(
        db.SmallInteger,
        nullable=False,
        default=DEFAULT_CREDITOR_STATUS,
        comment="Creditor's status bits: "
                f"{STATUS_IS_ACTIVATED_FLAG} - is activated, "
                f"{STATUS_IS_DEACTIVATED_FLAG} - is deactivated.",
    )

    created_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    reservation_id = db.Column(db.BigInteger, server_default=_ac_seq.next_value())
    last_log_entry_id = db.Column(db.BigInteger, nullable=False, default=0)
    largest_historic_ledger_entry_id = db.Column(db.BigInteger, nullable=False, server_default='0')
    creditor_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    creditor_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    accounts_list_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    accounts_list_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    transfers_list_latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    transfers_list_latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    deactivation_date = db.Column(
        db.DATE,
        comment='The date on which the creditor was deactivated. When a creditor gets '
                'deactivated, all its belonging objects (account, transfers, etc.) are '
                'removed. To be deactivated, the creditor must be activated first. Once '
                'deactivated, a creditor stays deactivated until it is deleted. A '
                '`NULL` value for this column means either that the creditor has not '
                'been deactivated yet, or that the deactivation date is unknown.',
    )
    __mapper_args__ = {'eager_defaults': True}
    __table_args__ = (
        db.CheckConstraint(creditor_id != ROOT_CREDITOR_ID),
        db.CheckConstraint(last_log_entry_id >= 0),
        db.CheckConstraint(creditor_latest_update_id > 0),
        db.CheckConstraint(accounts_list_latest_update_id > 0),
        db.CheckConstraint(transfers_list_latest_update_id > 0),
        db.CheckConstraint(or_(
            status_flags.op('&')(STATUS_IS_DEACTIVATED_FLAG) == 0,
            status_flags.op('&')(STATUS_IS_ACTIVATED_FLAG) != 0,
        )),
    )

    pin_info = db.relationship('PinInfo', uselist=False, cascade='all', passive_deletes=True)

    @property
    def is_activated(self):
        return bool(self.status_flags & Creditor.STATUS_IS_ACTIVATED_FLAG)

    @property
    def is_deactivated(self):
        return bool(self.status_flags & Creditor.STATUS_IS_DEACTIVATED_FLAG)

    def activate(self):
        self.status_flags |= Creditor.STATUS_IS_ACTIVATED_FLAG
        self.reservation_id = None

    def deactivate(self):
        self.status_flags |= Creditor.STATUS_IS_DEACTIVATED_FLAG
        self.deactivation_date = datetime.now(tz=timezone.utc).date()

    def generate_log_entry_id(self):
        self.last_log_entry_id += 1
        return self.last_log_entry_id


class PinInfo(db.Model):
    STATUS_OFF = 0
    STATUS_ON = 1
    STATUS_BLOCKED = 2

    STATUS_NAME_OFF = 'off'
    STATUS_NAME_ON = 'on'
    STATUS_NAME_BLOCKED = 'blocked'

    STATUS_NAMES = [STATUS_NAME_OFF, STATUS_NAME_ON, STATUS_NAME_BLOCKED]

    creditor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=STATUS_OFF,
        comment="PIN's status: "
                f"{STATUS_OFF} - off, "
                f"{STATUS_ON} - on, "
                f"{STATUS_BLOCKED} - blocked.",
    )
    cfa = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment='The number of consecutive failed attempts. It gets reset to zero when either '
                'of those events occur: 1) a correct PIN is entered; 2) the PIN is changed.',
    )
    afa = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment='The number of accumulated failed attempts. It gets reset to zero when either '
                'of those events occur: 1) some time has passed since the previous reset; 2) the '
                'PIN is blocked.',
    )
    afa_last_reset_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    pin_length = db.Column(db.SmallInteger, nullable=False, default=0)
    pin_hmac = db.Column(db.LargeBinary)
    latest_update_id = db.Column(db.BigInteger, nullable=False, default=1)
    latest_update_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint(and_(status >= 0, status < 3)),
        db.CheckConstraint(or_(status != STATUS_ON, pin_hmac != null())),
        db.CheckConstraint(cfa >= 0),
        db.CheckConstraint(afa >= 0),
        db.CheckConstraint(latest_update_id > 0),
        db.CheckConstraint(pin_length >= 0),
        db.CheckConstraint(or_(pin_hmac == null(), func.octet_length(pin_hmac) == 32)),
        {
            'comment': "Represents creditor's Personal Identification Number",
        }
    )

    @staticmethod
    def calc_hmac(secret: str, value: str) -> bytes:
        return hmac.digest(secret.encode('utf8'), value.encode('utf8'), 'sha256')

    @property
    def is_required(self) -> bool:
        return self.status != self.STATUS_OFF

    @property
    def status_name(self) -> str:
        return self.STATUS_NAMES[self.status]

    @status_name.setter
    def status_name(self, value: str) -> None:
        self.status = self.STATUS_NAMES.index(value)

    def _block(self):
        self.status = self.STATUS_BLOCKED
        self.pin_length = 0
        self.pin_hmac = None
        self._reset_afa(datetime.now(tz=timezone.utc))

    def _get_max_cfa(self) -> int:
        n = self.pin_length

        # NOTE: This is more or less an arbitrary linear
        # dependency. For `n` between 4 and 10, it generate the
        # sequence [3, 4, 6, 7, 9, 10, 12].
        return int(1.5 * n - 3)

    def _get_max_afa(self) -> int:
        n = self.pin_length

        # NOTE: This is more or less an arbitrary exponential
        # dependency. For `n` between 4 and 10, it generate the
        # sequence [10, 31 , 100, 316, 1000, 3162, 10000].
        return int(exp(1.1513 * (n - 2)))

    def _reset_afa(self, current_ts: datetime) -> None:
        self.afa = 0
        self.afa_last_reset_ts = current_ts

    def _reset_afa_if_necessary(self, reset_interval: timedelta) -> None:
        current_ts = datetime.now(tz=timezone.utc)
        if self.afa_last_reset_ts + reset_interval <= current_ts:
            self._reset_afa(current_ts)

    def _increment_cfa(self) -> None:
        self.cfa += 1
        if self.cfa >= self._get_max_cfa():
            self._block()

    def _increment_afa(self, reset_interval: timedelta) -> None:
        self._reset_afa_if_necessary(reset_interval)

        self.afa += 1
        if self.afa >= self._get_max_afa():
            self._block()

    def _register_failed_attempt(self, afa_reset_interval: timedelta) -> None:
        self._increment_cfa()
        self._increment_afa(afa_reset_interval)

    def _register_successful_attempt(self) -> None:
        self.cfa = 0

    def try_value(self, value: Optional[str], secret: str, afa_reset_interval: timedelta) -> bool:
        if self.status == self.STATUS_BLOCKED:
            return False

        if self.is_required:
            assert self.pin_hmac is not None
            if value is None:
                return False

            if PinInfo.calc_hmac(secret, value) != self.pin_hmac:
                self._register_failed_attempt(afa_reset_interval)
                return False

        self._register_successful_attempt()
        return True

    def set_value(self, value: Optional[str], secret: str) -> None:
        self.cfa = 0

        if value is None:
            self.pin_hmac = None
            self.pin_length = 0
        else:
            self.pin_hmac = self.calc_hmac(secret, value)
            self.pin_length = len(value)


class BaseLogEntry(db.Model):
    __abstract__ = True

    # Object type hints:
    OTH_TRANSFER = 1
    OTH_TRANSFERS_LIST = 2
    OTH_COMMITTED_TRANSFER = 3
    OTH_ACCOUNT_LEDGER = 4

    added_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    object_type = db.Column(db.String)
    object_uri = db.Column(db.String)
    object_update_id = db.Column(db.BigInteger)
    is_deleted = db.Column(db.BOOLEAN, comment='NULL has the same meaning as FALSE.')
    data = db.Column(pg.JSON)

    # NOTE: The following columns will be non-NULL for specific
    # `object_type`s only. They contain information allowing the
    # object's URI and type to be generated. Thus, the `object_uri`
    # and `object_type` columns can contain NULL for the most
    # frequently occuring log entries, saving space in the DB index.
    AUX_FIELDS = {
        'object_type_hint',
        'debtor_id',
        'creation_date',
        'transfer_number',
        'transfer_uuid',
    }
    object_type_hint = db.Column(db.SmallInteger)
    debtor_id = db.Column(db.BigInteger)
    creation_date = db.Column(db.DATE)
    transfer_number = db.Column(db.BigInteger)
    transfer_uuid = db.Column(pg.UUID(as_uuid=True))

    # NOTE: The following columns will be non-NULL for specific
    # `object_type`s only. They contain information allowing the
    # object's JSON data to be generated. Thus, the `data `column can
    # contain NULL for the most frequently occuring log entries,
    # saving space in the DB index.
    DATA_FIELDS = {
        # The key is the name of the column in the table, the value is
        # the name of the corresponding JSON property in the `data`
        # dictionary.
        'data_principal': 'principal',
        'data_next_entry_id': 'nextEntryId',
        'data_finalized_at': 'finalizedAt',
        'data_error_code': 'errorCode',
    }
    data_principal = db.Column(db.BigInteger)
    data_next_entry_id = db.Column(db.BigInteger)
    data_finalized_at = db.Column(db.TIMESTAMP(timezone=True))
    data_error_code = db.Column(db.String)

    @property
    def is_created(self):
        return not self.is_deleted and self.object_update_id in [1, None]

    def get_object_type(self, types) -> str:
        object_type = self.object_type
        if object_type is not None:
            return object_type

        object_type_hint = self.object_type_hint

        if object_type_hint == self.OTH_TRANSFER:
            return types.transfer
        elif object_type_hint == self.OTH_TRANSFERS_LIST:
            return types.transfers_list
        elif object_type_hint == self.OTH_COMMITTED_TRANSFER:
            return types.committed_transfer
        elif object_type_hint == self.OTH_ACCOUNT_LEDGER:
            return types.account_ledger

        logger = logging.getLogger(__name__)
        logger.error('Log entry without an object type.')
        return 'object'

    def get_object_uri(self, paths) -> str:
        object_uri = self.object_uri
        if object_uri is not None:
            return object_uri

        object_type_hint = self.object_type_hint

        if object_type_hint == self.OTH_TRANSFER:
            transfer_uuid = self.transfer_uuid
            if transfer_uuid is not None:
                return paths.transfer(
                    creditorId=self.creditor_id,
                    transferUuid=transfer_uuid,
                )
        elif object_type_hint == self.OTH_TRANSFERS_LIST:
            return paths.transfers_list(creditorId=self.creditor_id)
        elif object_type_hint == self.OTH_COMMITTED_TRANSFER:
            debtor_id = self.debtor_id
            creation_date = self.creation_date
            transfer_number = self.transfer_number
            if debtor_id is not None and creation_date is not None and transfer_number is not None:
                return paths.committed_transfer(
                    creditorId=self.creditor_id,
                    debtorId=debtor_id,
                    creationDate=creation_date,
                    transferNumber=transfer_number,
                )
        elif object_type_hint == self.OTH_ACCOUNT_LEDGER:
            debtor_id = self.debtor_id
            if debtor_id is not None:
                return paths.account_ledger(
                    creditorId=self.creditor_id,
                    debtorId=self.debtor_id,
                )

        logger = logging.getLogger(__name__)
        logger.error('Log entry without an object URI.')
        return ''

    def get_data_dict(self) -> Optional[Dict]:
        if isinstance(self.data, dict):
            return self.data

        items = self.DATA_FIELDS.items()
        data = {prop: self._jsonify_attribute(attr) for attr, prop in items if getattr(self, attr) is not None}
        return data or None

    def _jsonify_attribute(self, attr_name):
        value = getattr(self, attr_name)
        if isinstance(value, datetime):
            return value.isoformat()
        return value


class PendingLogEntry(BaseLogEntry):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    pending_entry_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    __table_args__ = (
        db.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
        db.CheckConstraint('object_update_id > 0'),
        db.CheckConstraint('transfer_number > 0'),
        db.CheckConstraint('data_next_entry_id > 0'),
        {
            'comment': 'Represents a log entry that should be added to the log. Adding entries '
                       'to the creditor\'s log requires a lock on the `creditor` table row. To '
                       'avoid obtaining the lock too often, log entries are queued to this table, '
                       'allowing many log entries for one creditor to be added to the log in '
                       'a single database transaction, thus reducing the lock contention.',
        }
    )


class LogEntry(BaseLogEntry):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    entry_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The rest of the columns are not be part of the primary
    # key, but should be included in the primary key index to allow
    # index-only scans. Because SQLAlchemy does not support this yet
    # (2020-01-11), the migration file should be edited so as not to
    # create a "normal" index, but create a "covering" index instead.

    __table_args__ = (
        db.CheckConstraint('object_update_id > 0'),
        db.CheckConstraint('transfer_number > 0'),
        db.CheckConstraint('data_next_entry_id > 0'),
        db.CheckConstraint(entry_id > 0),
    )
