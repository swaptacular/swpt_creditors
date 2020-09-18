from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from sqlalchemy.orm import load_only
from sqlalchemy.dialects import postgresql
from swpt_lib.scan_table import TableScanner
from .extensions import db
from .models import AccountData, PendingLogEntry, LedgerEntry, PendingLedgerUpdate
from .procedures import contain_principal_overflow, get_paths_and_types, ACCOUNT_DATA_LEDGER_RELATED_COLUMNS

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
TD_HOUR = timedelta(hours=1)
INSERT_PENDING_LEDGER_UPDATE_STATEMENT = postgresql.insert(PendingLedgerUpdate.__table__).on_conflict_do_nothing()

# TODO: Consider making `TableScanner.blocks_per_query` and
#       `TableScanner.target_beat_duration` configurable.


class AccountScanner(TableScanner):
    """Performs accounts maintenance operations."""

    table = AccountData.__table__
    pk = tuple_(AccountData.creditor_id, AccountData.debtor_id)

    def __init__(self):
        self.max_transfer_delay = timedelta(days=current_app.config['APP_MAX_TRANSFER_DELAY_DAYS'])
        super().__init__()

    @atomic
    def process_rows(self, rows):
        self._update_ledgers_if_necessary(rows)
        self._schedule_ledger_repairs_if_necessary(rows)

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
            db.session.execute(INSERT_PENDING_LEDGER_UPDATE_STATEMENT, [
                {'creditor_id': creditor_id, 'debtor_id': debtor_id}
                for creditor_id, debtor_id in pks_to_repair
            ])
