from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from flask import current_app
from sqlalchemy.sql.expression import tuple_, or_, and_, false, true, null
from sqlalchemy.orm import load_only
from sqlalchemy.dialects import postgresql
from swpt_pythonlib.scan_table import TableScanner
from .extensions import db
from .models import (
    Creditor,
    AccountData,
    PendingLogEntry,
    LogEntry,
    LedgerEntry,
    CommittedTransfer,
    PendingLedgerUpdate,
    UpdatedLedgerSignal,
    uid_seq,
    is_valid_creditor_id,
)
from .procedures import (
    contain_principal_overflow,
    get_paths_and_types,
    LOAD_ONLY_LEDGER_RELATED_COLUMNS,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

TD_HOUR = timedelta(hours=1)
ENSURE_PENDING_LEDGER_UPDATE_STATEMENT = postgresql.insert(
    PendingLedgerUpdate.__table__
).on_conflict_do_nothing()


class CreditorScanner(TableScanner):
    """Garbage-collects inactive creditors."""

    table = Creditor.__table__
    columns = [
        Creditor.creditor_id,
        Creditor.created_at,
        Creditor.status_flags,
        Creditor.deactivation_date,
    ]
    pk = tuple_(
        table.c.creditor_id,
    )

    def __init__(self):
        super().__init__()
        self.inactive_interval = timedelta(
            days=current_app.config["APP_INACTIVE_CREDITOR_RETENTION_DAYS"]
        )
        self.deactivated_interval = timedelta(
            days=current_app.config["APP_DEACTIVATED_CREDITOR_RETENTION_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config["APP_CREDITORS_SCAN_BLOCKS_PER_QUERY"]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config["APP_CREDITORS_SCAN_BEAT_MILLISECS"]

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_creditors(rows, current_ts)
        self._delete_creditors_not_activated_for_long_time(rows, current_ts)
        self._delete_creditors_deactivated_long_time_ago(rows, current_ts)

    def _delete_creditors_not_activated_for_long_time(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_status_flags = c.status_flags
        c_created_at = c.created_at
        activated_flag = Creditor.STATUS_IS_ACTIVATED_FLAG
        inactive_cutoff_ts = current_ts - self.inactive_interval

        def not_activated_for_long_time(row) -> bool:
            return (
                row[c_status_flags] & activated_flag == 0
                and row[c_created_at] < inactive_cutoff_ts
            )

        ids_to_delete = [
            row[c_creditor_id]
            for row in rows
            if not_activated_for_long_time(row)
        ]
        if ids_to_delete:
            to_delete = (
                Creditor.query
                .options(load_only(Creditor.creditor_id))
                .filter(Creditor.creditor_id.in_(ids_to_delete))
                .filter(Creditor.status_flags.op("&")(activated_flag) == 0)
                .filter(Creditor.created_at < inactive_cutoff_ts)
                .with_for_update(skip_locked=True)
                .all()
            )

            for creditor in to_delete:
                db.session.delete(creditor)

            db.session.commit()

    def _delete_creditors_deactivated_long_time_ago(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_status_flags = c.status_flags
        c_deactivation_date = c.deactivation_date
        deactivated_flag = Creditor.STATUS_IS_DEACTIVATED_FLAG
        deactivated_cutoff_date = (
            current_ts - self.deactivated_interval
        ).date()

        def deactivated_long_time_ago(row) -> bool:
            return row[c_status_flags] & deactivated_flag != 0 and (
                row[c_deactivation_date] is None
                or row[c_deactivation_date] < deactivated_cutoff_date
            )

        ids_to_delete = [
            row[c_creditor_id]
            for row in rows
            if deactivated_long_time_ago(row)
        ]
        if ids_to_delete:
            to_delete = (
                Creditor.query
                .options(load_only(Creditor.creditor_id))
                .filter(Creditor.creditor_id.in_(ids_to_delete))
                .filter(Creditor.status_flags.op("&")(deactivated_flag) != 0)
                .filter(
                    or_(
                        Creditor.deactivation_date == null(),
                        Creditor.deactivation_date < deactivated_cutoff_date,
                    ),
                )
                .with_for_update(skip_locked=True)
                .all()
            )

            for creditor in to_delete:
                db.session.delete(creditor)

            db.session.commit()

    def _delete_parent_shard_creditors(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id

        def belongs_to_parent_shard(row) -> bool:
            return not is_valid_creditor_id(
                row[c_creditor_id]
            ) and is_valid_creditor_id(row[c_creditor_id], match_parent=True)

        ids_to_delete = [
            row[c_creditor_id] for row in rows if belongs_to_parent_shard(row)
        ]
        if ids_to_delete:
            to_delete = (
                Creditor.query
                .options(load_only(Creditor.creditor_id))
                .filter(Creditor.creditor_id.in_(ids_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for creditor in to_delete:
                db.session.delete(creditor)

            db.session.commit()


class LogEntryScanner(TableScanner):
    """Garbage-collects staled log entries."""

    table = LogEntry.__table__
    columns = [LogEntry.creditor_id, LogEntry.entry_id, LogEntry.added_at]
    process_individual_blocks = True
    pk = tuple_(table.c.creditor_id, table.c.entry_id)
    MIN_DELETABLE_GROUP = 25  # ~2/3 of the maximum number of rows in the page

    def __init__(self):
        super().__init__()
        self.retention_interval = timedelta(
            days=current_app.config["APP_LOG_RETENTION_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return int(current_app.config["APP_LOG_ENTRIES_SCAN_BLOCKS_PER_QUERY"])

    @property
    def target_beat_duration(self) -> int:
        return int(current_app.config["APP_LOG_ENTRIES_SCAN_BEAT_MILLISECS"])

    @atomic
    def process_rows(self, rows):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_entry_id = c.entry_id
        c_added_at = c.added_at
        delete_parent_shard_records = current_app.config[
            "DELETE_PARENT_SHARD_RECORDS"
        ]
        cutoff_ts = datetime.now(tz=timezone.utc) - self.retention_interval

        pks_to_delete = [
            (row[c_creditor_id], row[c_entry_id])
            for row in rows
            if row[c_added_at] < cutoff_ts
            or (
                delete_parent_shard_records
                and not is_valid_creditor_id(row[c_creditor_id])
            )
        ]
        this_page_contains_lots_of_deletable_rows = (
            # We do not want to remove this page from the visibility
            # map only because a few of the tuples in the page are
            # dead. Instead, we will wait until most of the rows can
            # be killed.
            len(pks_to_delete) >= self.MIN_DELETABLE_GROUP
        )
        if this_page_contains_lots_of_deletable_rows:
            db.session.execute(
                self.table.delete().where(self.pk.in_(pks_to_delete))
            )
            db.session.commit()


class LedgerEntryScanner(TableScanner):
    """Garbage-collects staled ledger entries."""

    table = LedgerEntry.__table__
    columns = [
        LedgerEntry.creditor_id,
        LedgerEntry.debtor_id,
        LedgerEntry.entry_id,
        LedgerEntry.added_at,
    ]
    process_individual_blocks = True
    pk = tuple_(table.c.creditor_id, table.c.debtor_id, table.c.entry_id)
    MIN_DELETABLE_GROUP = 50  # ~2/3 of the maximum number of rows in the page

    def __init__(self):
        super().__init__()
        self.retention_interval = timedelta(
            days=current_app.config["APP_LEDGER_RETENTION_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return int(
            current_app.config["APP_LEDGER_ENTRIES_SCAN_BLOCKS_PER_QUERY"]
        )

    @property
    def target_beat_duration(self) -> int:
        return int(
            current_app.config["APP_LEDGER_ENTRIES_SCAN_BEAT_MILLISECS"]
        )

    @atomic
    def process_rows(self, rows):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_entry_id = c.entry_id
        c_added_at = c.added_at
        delete_parent_shard_records = current_app.config[
            "DELETE_PARENT_SHARD_RECORDS"
        ]
        cutoff_ts = datetime.now(tz=timezone.utc) - self.retention_interval

        pks_to_delete = [
            (row[c_creditor_id], row[c_debtor_id], row[c_entry_id])
            for row in rows
            if row[c_added_at] < cutoff_ts
            or (
                delete_parent_shard_records
                and not is_valid_creditor_id(row[c_creditor_id])
            )
        ]
        this_page_contains_lots_of_deletable_rows = (
            # We do not want to remove this page from the visibility
            # map only because a few of the tuples in the page are
            # dead. Instead, we will wait until most of the rows can
            # be killed.
            len(pks_to_delete) >= self.MIN_DELETABLE_GROUP
        )
        if this_page_contains_lots_of_deletable_rows:
            db.session.execute(
                self.table.delete().where(self.pk.in_(pks_to_delete))
            )
            db.session.commit()


class CommittedTransferScanner(TableScanner):
    """Garbage-collects staled committed transfers."""

    table = CommittedTransfer.__table__
    columns = [
        CommittedTransfer.creditor_id,
        CommittedTransfer.debtor_id,
        CommittedTransfer.creation_date,
        CommittedTransfer.transfer_number,
        CommittedTransfer.committed_at,
    ]
    process_individual_blocks = True
    pk = tuple_(
        table.c.creditor_id,
        table.c.debtor_id,
        table.c.creation_date,
        table.c.transfer_number,
    )
    MIN_DELETABLE_GROUP = 25  # ~2/3 of the maximum number of rows in the page

    def __init__(self):
        super().__init__()
        self.retention_interval = timedelta(
            days=current_app.config["APP_MAX_TRANSFER_DELAY_DAYS"]
        ) + max(
            timedelta(days=current_app.config["APP_LOG_RETENTION_DAYS"]),
            timedelta(days=current_app.config["APP_LEDGER_RETENTION_DAYS"]),
        )

    @property
    def blocks_per_query(self) -> int:
        return int(
            current_app.config["APP_COMMITTED_TRANSFERS_SCAN_BLOCKS_PER_QUERY"]
        )

    @property
    def target_beat_duration(self) -> int:
        return int(
            current_app.config["APP_COMMITTED_TRANSFERS_SCAN_BEAT_MILLISECS"]
        )

    @atomic
    def process_rows(self, rows):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_creation_date = c.creation_date
        c_transfer_number = c.transfer_number
        c_committed_at = c.committed_at
        delete_parent_shard_records = current_app.config[
            "DELETE_PARENT_SHARD_RECORDS"
        ]
        cutoff_ts = datetime.now(tz=timezone.utc) - self.retention_interval

        pks_to_delete = [
            (
                row[c_creditor_id],
                row[c_debtor_id],
                row[c_creation_date],
                row[c_transfer_number],
            )
            for row in rows
            if row[c_committed_at] < cutoff_ts
            or (
                delete_parent_shard_records
                and not is_valid_creditor_id(row[c_creditor_id])
            )
        ]
        this_page_contains_lots_of_deletable_rows = (
            # We do not want to remove this page from the visibility
            # map only because a few of the tuples in the page are
            # dead. Instead, we will wait until most of the rows can
            # be killed.
            len(pks_to_delete) >= self.MIN_DELETABLE_GROUP
        )
        if this_page_contains_lots_of_deletable_rows:
            db.session.execute(
                self.table.delete().where(self.pk.in_(pks_to_delete))
            )
            db.session.commit()


class AccountScanner(TableScanner):
    """Performs accounts maintenance operations."""

    table = AccountData.__table__
    columns = [
        AccountData.creditor_id,
        AccountData.debtor_id,
        AccountData.ledger_latest_update_ts,
        AccountData.ledger_pending_transfer_ts,
        AccountData.last_transfer_number,
        AccountData.ledger_last_transfer_number,
        AccountData.principal,
        AccountData.ledger_principal,
        AccountData.last_transfer_number,
        AccountData.ledger_last_transfer_number,
        AccountData.last_transfer_committed_at,
        AccountData.is_config_effectual,
        AccountData.has_server_account,
        AccountData.last_heartbeat_ts,
        AccountData.config_error,
        AccountData.last_config_ts,
    ]
    pk = tuple_(AccountData.creditor_id, AccountData.debtor_id)

    def __init__(self):
        super().__init__()
        self.max_heartbeat_delay = timedelta(
            days=current_app.config["APP_MAX_HEARTBEAT_DELAY_DAYS"]
        )
        self.max_transfer_delay = timedelta(
            days=current_app.config["APP_MAX_TRANSFER_DELAY_DAYS"]
        )
        self.max_config_delay = timedelta(
            hours=current_app.config["APP_MAX_CONFIG_DELAY_HOURS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return int(current_app.config["APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY"])

    @property
    def target_beat_duration(self) -> int:
        return int(current_app.config["APP_ACCOUNTS_SCAN_BEAT_MILLISECS"])

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        self._update_ledgers_if_necessary(rows, current_ts)
        self._schedule_ledger_repairs_if_necessary(rows, current_ts)
        self._set_config_errors_if_necessary(rows, current_ts)

    def _update_ledgers_if_necessary(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_last_transfer_number = c.last_transfer_number
        c_ledger_last_transfer_number = c.ledger_last_transfer_number
        c_ledger_principal = c.ledger_principal
        c_principal = c.principal
        c_ledger_latest_update_ts = c.ledger_latest_update_ts
        latest_update_cutoff_ts = current_ts - TD_HOUR

        def needs_update(row) -> bool:
            return (
                row[c_last_transfer_number]
                == row[c_ledger_last_transfer_number]
                and row[c_ledger_principal] != row[c_principal]
                and row[c_ledger_latest_update_ts] < latest_update_cutoff_ts
            )

        pks_to_update = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if needs_update(row) and is_valid_creditor_id(row[c_creditor_id])
        ]
        if pks_to_update:
            ledger_update_pending_log_entries = []

            to_update = (
                AccountData.query
                .options(LOAD_ONLY_LEDGER_RELATED_COLUMNS)
                .filter(self.pk.in_(pks_to_update))
                .filter(
                    AccountData.last_transfer_number
                    == AccountData.ledger_last_transfer_number
                )
                .filter(AccountData.ledger_principal != AccountData.principal)
                .filter(
                    AccountData.ledger_latest_update_ts
                    < latest_update_cutoff_ts
                )
                .with_for_update(skip_locked=True, key_share=True)
                .all()
            )

            for data in to_update:
                log_entry = self._update_ledger(data, current_ts)
                ledger_update_pending_log_entries.append(log_entry)

            db.session.bulk_save_objects(
                ledger_update_pending_log_entries, preserve_order=False
            )
            db.session.scalar(uid_seq)
            db.session.commit()

    def _update_ledger(
        self, data: AccountData, current_ts: datetime
    ) -> PendingLogEntry:
        creditor_id = data.creditor_id
        debtor_id = data.debtor_id
        principal = data.principal
        ledger_principal = data.ledger_principal
        ledger_last_entry_id = data.ledger_last_entry_id
        correction_amount = principal - ledger_principal

        assert correction_amount != 0
        while correction_amount != 0:
            safe_correction_amount = contain_principal_overflow(
                correction_amount
            )
            correction_amount -= safe_correction_amount
            ledger_principal += safe_correction_amount
            ledger_last_entry_id += 1
            db.session.add(
                LedgerEntry(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    entry_id=ledger_last_entry_id,
                    acquired_amount=safe_correction_amount,
                    principal=ledger_principal,
                    added_at=current_ts,
                )
            )

        data.ledger_last_entry_id = ledger_last_entry_id
        data.ledger_principal = principal
        data.ledger_latest_update_id += 1
        data.ledger_latest_update_ts = current_ts

        db.session.add(UpdatedLedgerSignal(
            creditor_id=data.creditor_id,
            debtor_id=data.debtor_id,
            update_id=data.ledger_latest_update_id,
            account_id=data.account_id,
            creation_date=data.creation_date,
            principal=principal,
            last_transfer_number=data.ledger_last_transfer_number,
            ts=current_ts,
        ))

        return PendingLogEntry(
            creditor_id=creditor_id,
            added_at=current_ts,
            object_type_hint=LogEntry.OTH_ACCOUNT_LEDGER,
            debtor_id=debtor_id,
            object_update_id=data.ledger_latest_update_id,
            data_principal=principal,
            data_next_entry_id=data.ledger_last_entry_id + 1,
        )

    def _schedule_ledger_repairs_if_necessary(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_last_transfer_number = c.last_transfer_number
        c_ledger_last_transfer_number = c.ledger_last_transfer_number
        c_ledger_pending_transfer_ts = c.ledger_pending_transfer_ts
        c_last_transfer_committed_at = c.last_transfer_committed_at
        committed_at_cutoff = current_ts - self.max_transfer_delay

        def needs_repair(row) -> bool:
            if (
                row[c_last_transfer_number]
                <= row[c_ledger_last_transfer_number]
            ):
                return False
            ledger_pending_transfer_ts = row[c_ledger_pending_transfer_ts]
            if ledger_pending_transfer_ts is not None:
                return ledger_pending_transfer_ts < committed_at_cutoff
            return row[c_last_transfer_committed_at] < committed_at_cutoff

        pks_to_repair = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if needs_repair(row) and is_valid_creditor_id(row[c_creditor_id])
        ]
        if pks_to_repair:
            db.session.execute(
                ENSURE_PENDING_LEDGER_UPDATE_STATEMENT,
                [
                    {"creditor_id": creditor_id, "debtor_id": debtor_id}
                    for creditor_id, debtor_id in pks_to_repair
                ],
            )
            db.session.commit()

    def _set_config_errors_if_necessary(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_is_config_effectual = c.is_config_effectual
        c_has_server_account = c.has_server_account
        c_last_heartbeat_ts = c.last_heartbeat_ts
        c_config_error = c.config_error
        c_last_config_ts = c.last_config_ts
        last_heartbeat_ts_cutoff = current_ts - self.max_heartbeat_delay
        last_config_ts_cutoff = current_ts - self.max_config_delay

        def has_unreported_config_problem(row) -> bool:
            return (
                (
                    not row[c_is_config_effectual]
                    or (
                        row[c_has_server_account]
                        and row[c_last_heartbeat_ts] < last_heartbeat_ts_cutoff
                    )
                )
                and row[c_config_error] is None
                and row[c_last_config_ts] < last_config_ts_cutoff
            )

        pks_to_set = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if (
                has_unreported_config_problem(row)
                and is_valid_creditor_id(row[c_creditor_id])
            )
        ]
        if pks_to_set:
            info_update_pending_log_entries = []

            to_set = (
                AccountData.query
                .options(load_only(AccountData.info_latest_update_id))
                .filter(self.pk.in_(pks_to_set))
                .filter(
                    or_(
                        AccountData.is_config_effectual == false(),
                        and_(
                            AccountData.has_server_account == true(),
                            AccountData.last_heartbeat_ts
                            < last_heartbeat_ts_cutoff,
                        ),
                    )
                )
                .filter(AccountData.config_error == null())
                .filter(AccountData.last_config_ts < last_config_ts_cutoff)
                .with_for_update(skip_locked=True, key_share=True)
                .all()
            )

            for data in to_set:
                log_entry = self._set_config_error(data, current_ts)
                info_update_pending_log_entries.append(log_entry)

            db.session.bulk_save_objects(
                info_update_pending_log_entries, preserve_order=False
            )
            db.session.scalar(uid_seq)
            db.session.commit()

    def _set_config_error(
        self, data: AccountData, current_ts: datetime
    ) -> PendingLogEntry:
        data.config_error = "CONFIGURATION_IS_NOT_EFFECTUAL"
        data.info_latest_update_id += 1
        data.info_latest_update_ts = current_ts

        paths, types = get_paths_and_types()
        return PendingLogEntry(
            creditor_id=data.creditor_id,
            added_at=current_ts,
            object_type=types.account_info,
            object_uri=paths.account_info(
                creditorId=data.creditor_id, debtorId=data.debtor_id
            ),
            object_update_id=data.info_latest_update_id,
        )
