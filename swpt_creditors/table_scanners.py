from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from flask import current_app
from sqlalchemy.sql.expression import tuple_, or_, and_, false, true, null
from sqlalchemy.orm import load_only
from sqlalchemy.dialects import postgresql
from swpt_lib.scan_table import TableScanner
from .extensions import db
from .models import AccountData, PendingLogEntry, LogEntry, LedgerEntry, CommittedTransfer, PendingLedgerUpdate
from .procedures import contain_principal_overflow, get_paths_and_types, \
    ACCOUNT_DATA_LEDGER_RELATED_COLUMNS, ACCOUNT_DATA_CONFIG_RELATED_COLUMNS

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

TD_HOUR = timedelta(hours=1)
ENSURE_PENDING_LEDGER_UPDATE_STATEMENT = postgresql.insert(PendingLedgerUpdate.__table__).on_conflict_do_nothing()

# TODO: Consider making `TableScanner.blocks_per_query` and
#       `TableScanner.target_beat_duration` configurable.


class LogEntriesScanner(TableScanner):
    """Garbage-collects staled log entries."""

    table = LogEntry.__table__
    columns = [LogEntry.creditor_id, LogEntry.entry_id, LogEntry.added_at_ts]
    pk = tuple_(table.c.creditor_id, table.c.entry_id)

    def __init__(self):
        super().__init__()
        self.retention_interval = timedelta(days=current_app.config['APP_LOG_RETENTION_DAYS'])

    @atomic
    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - self.retention_interval
        pks_to_delete = [(row[0], row[1]) for row in rows if row[2] < cutoff_ts]
        if pks_to_delete:
            db.session.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class LedgerEntriesScanner(TableScanner):
    """Garbage-collects staled ledger entries."""

    table = LedgerEntry.__table__
    columns = [LedgerEntry.creditor_id, LedgerEntry.debtor_id, LedgerEntry.entry_id, LedgerEntry.added_at_ts]
    pk = tuple_(table.c.creditor_id, table.c.debtor_id, table.c.entry_id)

    def __init__(self):
        super().__init__()
        self.retention_interval = timedelta(days=current_app.config['APP_LEDGER_RETENTION_DAYS'])

    @atomic
    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - self.retention_interval
        pks_to_delete = [(row[0], row[1], row[2]) for row in rows if row[3] < cutoff_ts]
        if pks_to_delete:
            db.session.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class CommittedTransfersScanner(TableScanner):
    """Garbage-collects staled committed transfers."""

    table = CommittedTransfer.__table__
    columns = [
        CommittedTransfer.creditor_id,
        CommittedTransfer.debtor_id,
        CommittedTransfer.creation_date,
        CommittedTransfer.transfer_number,
        CommittedTransfer.committed_at_ts,
    ]
    pk = tuple_(
        table.c.creditor_id,
        table.c.debtor_id,
        table.c.creation_date,
        table.c.transfer_number,
    )

    def __init__(self):
        super().__init__()
        self.retention_interval = timedelta(days=current_app.config['APP_MAX_TRANSFER_DELAY_DAYS']) + max(
            timedelta(days=current_app.config['APP_LOG_RETENTION_DAYS']),
            timedelta(days=current_app.config['APP_LEDGER_RETENTION_DAYS']),
        )

    @atomic
    def process_rows(self, rows):
        cutoff_ts = datetime.now(tz=timezone.utc) - self.retention_interval
        pks_to_delete = [(row[0], row[1], row[2], row[3]) for row in rows if row[4] < cutoff_ts]
        if pks_to_delete:
            db.session.execute(self.table.delete().where(self.pk.in_(pks_to_delete)))


class AccountScanner(TableScanner):
    """Performs accounts maintenance operations."""

    table = AccountData.__table__
    pk = tuple_(AccountData.creditor_id, AccountData.debtor_id)

    def __init__(self):
        self.max_heartbeat_delay = timedelta(days=current_app.config['APP_MAX_HEARTBEAT_DELAY_DAYS'])
        self.max_transfer_delay = timedelta(days=current_app.config['APP_MAX_TRANSFER_DELAY_DAYS'])
        self.max_config_delay = timedelta(hours=current_app.config['APP_MAX_CONFIG_DELAY_HOURS'])
        super().__init__()

    @atomic
    def process_rows(self, rows):
        self._update_ledgers_if_necessary(rows)
        self._schedule_ledger_repairs_if_necessary(rows)
        self._set_config_errors_if_necessary(rows)

    def _update_ledgers_if_necessary(self, rows):
        c = self.table.c
        current_ts = datetime.now(tz=timezone.utc)
        latest_update_cutoff_ts = current_ts - TD_HOUR

        def needs_update(row) -> bool:
            return (
                row[c.last_transfer_number] == row[c.ledger_last_transfer_number]
                and row[c.ledger_principal] != row[c.principal]
                and row[c.ledger_latest_update_ts] < latest_update_cutoff_ts
            )

        pks_to_update = [(row[c.creditor_id], row[c.debtor_id]) for row in rows if needs_update(row)]
        if pks_to_update:
            ledger_update_pending_log_entries = []
            to_update = AccountData.query.\
                filter(self.pk.in_(pks_to_update)).\
                filter(AccountData.last_transfer_number == AccountData.ledger_last_transfer_number).\
                filter(AccountData.ledger_principal != AccountData.principal).\
                filter(AccountData.ledger_latest_update_ts < latest_update_cutoff_ts).\
                with_for_update().\
                options(load_only(*ACCOUNT_DATA_LEDGER_RELATED_COLUMNS)).\
                all()
            for data in to_update:
                log_entry = self._update_ledger(data, current_ts)
                ledger_update_pending_log_entries.append(log_entry)

            db.session.bulk_save_objects(ledger_update_pending_log_entries, preserve_order=False)

    def _update_ledger(self, data: AccountData, current_ts: datetime) -> PendingLogEntry:
        assert data.last_transfer_number == data.ledger_last_transfer_number
        assert data.principal != data.ledger_principal
        creditor_id = data.creditor_id
        debtor_id = data.debtor_id
        principal = data.principal
        ledger_principal = data.ledger_principal
        correction_amount = principal - ledger_principal

        while correction_amount != 0:
            safe_correction_amount = contain_principal_overflow(correction_amount)
            correction_amount -= safe_correction_amount
            ledger_principal += safe_correction_amount
            data.ledger_last_entry_id += 1
            db.session.add(LedgerEntry(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                entry_id=data.ledger_last_entry_id,
                aquired_amount=safe_correction_amount,
                principal=ledger_principal,
                added_at_ts=current_ts,
            ))

        data.ledger_principal = principal
        data.ledger_pending_transfer_ts = None
        data.ledger_latest_update_id += 1
        data.ledger_latest_update_ts = current_ts
        paths, types = get_paths_and_types()

        return PendingLogEntry(
            creditor_id=creditor_id,
            added_at_ts=current_ts,
            object_type=types.account_ledger,
            object_uri=paths.account_ledger(creditorId=creditor_id, debtorId=debtor_id),
            object_update_id=data.ledger_latest_update_id,
            data_principal=principal,
            data_next_entry_id=data.ledger_last_entry_id + 1,
        )

    def _schedule_ledger_repairs_if_necessary(self, rows):
        c = self.table.c
        committed_at_cutoff = datetime.now(tz=timezone.utc) - self.max_transfer_delay

        def needs_repair(row) -> bool:
            if row[c.last_transfer_number] <= row[c.ledger_last_transfer_number]:
                return False
            ledger_pending_transfer_ts = row[c.ledger_pending_transfer_ts]
            if ledger_pending_transfer_ts is not None:
                return ledger_pending_transfer_ts < committed_at_cutoff
            return row[c.last_transfer_ts] < committed_at_cutoff

        pks_to_repair = [(row[c.creditor_id], row[c.debtor_id]) for row in rows if needs_repair(row)]
        if pks_to_repair:
            db.session.execute(ENSURE_PENDING_LEDGER_UPDATE_STATEMENT, [
                {'creditor_id': creditor_id, 'debtor_id': debtor_id}
                for creditor_id, debtor_id in pks_to_repair
            ])

    def _set_config_errors_if_necessary(self, rows):
        c = self.table.c
        current_ts = datetime.now(tz=timezone.utc)
        last_heartbeat_ts_cutoff = current_ts - self.max_heartbeat_delay
        last_config_ts_cutoff = current_ts - self.max_config_delay

        def has_config_problem(row) -> bool:
            return (
                (not row[c.is_config_effectual]
                 or row[c.has_server_account] and row[c.last_heartbeat_ts] < last_heartbeat_ts_cutoff)
                and row[c.config_error] is None
                and row[c.last_config_ts] < last_config_ts_cutoff
            )

        pks_to_set = [(row[c.creditor_id], row[c.debtor_id]) for row in rows if has_config_problem(row)]
        if pks_to_set:
            info_update_pending_log_entries = []
            to_set = AccountData.query.\
                filter(self.pk.in_(pks_to_set)).\
                filter(or_(
                    AccountData.is_config_effectual == false(),
                    and_(
                        AccountData.has_server_account == true(),
                        AccountData.last_heartbeat_ts < last_heartbeat_ts_cutoff,
                    ),
                )).\
                filter(AccountData.config_error == null()).\
                filter(AccountData.last_config_ts < last_config_ts_cutoff).\
                with_for_update().\
                options(load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)).\
                all()
            for data in to_set:
                log_entry = self._set_config_error(data, current_ts)
                info_update_pending_log_entries.append(log_entry)

            db.session.bulk_save_objects(info_update_pending_log_entries, preserve_order=False)

    def _set_config_error(self, data: AccountData, current_ts: datetime) -> PendingLogEntry:
        data.config_error = 'CONFIGURATION_IS_NOT_EFFECTUAL'
        data.info_latest_update_id += 1
        data.info_latest_update_ts = current_ts
        paths, types = get_paths_and_types()

        return PendingLogEntry(
            creditor_id=data.creditor_id,
            added_at_ts=current_ts,
            object_type=types.account_info,
            object_uri=paths.account_info(creditorId=data.creditor_id, debtorId=data.debtor_id),
            object_update_id=data.info_latest_update_id,
        )
