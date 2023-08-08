from datetime import date, timedelta
from swpt_creditors.extensions import db
from swpt_creditors import procedures as p
from swpt_creditors import models as m

D_ID = -1
C_ID = 4294967296


def _create_new_creditor(creditor_id: int, activate: bool = False):
    creditor = p.reserve_creditor(creditor_id)
    if activate:
        p.activate_creditor(creditor_id, str(creditor.reservation_id))


def test_process_ledger_entries(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    m.Account.query.delete()
    m.LogEntry.query.delete()
    m.LedgerEntry.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    m.CommittedTransfer.query.delete()
    db.session.commit()

    app = app_unsafe_session
    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)

    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': date(2020, 1, 1),
        'last_change_ts': current_ts,
        'last_change_seqnum': 1,
        'principal': 1000,
        'interest': 0.0,
        'interest_rate': 5.0,
        'last_interest_rate_change_ts': current_ts,
        'transfer_note_max_bytes': 500,
        'last_config_ts': current_ts,
        'last_config_seqnum': 1,
        'negligible_amount': 0.0,
        'config_flags': 0,
        'config_data': '',
        'account_id': str(C_ID),
        'debtor_info_iri': 'http://example.com',
        'debtor_info_content_type': None,
        'debtor_info_sha256': None,
        'last_transfer_number': 0,
        'last_transfer_committed_at': current_ts,
        'ts': current_ts,
        'ttl': 100000,
    }
    p.process_account_update_signal(**params)

    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': date(2020, 1, 1),
        'transfer_number': 1,
        'coordinator_type': 'direct',
        'sender': '666',
        'recipient': str(C_ID),
        'acquired_amount': 200,
        'transfer_note_format': 'json',
        'transfer_note': '{"message": "test"}',
        'committed_at': current_ts,
        'principal': 200,
        'ts': current_ts,
        'previous_transfer_number': 0,
        'retention_interval': timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)
    params['transfer_number'] = 2
    params['principal'] = 400
    params['previous_transfer_number'] = 1
    p.process_account_transfer_signal(**params)

    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=10000, count=10000)) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'process_ledger_updates', '--burst=1', '--quit-early', '--wait=0'])
    assert not result.output
    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=10000, count=10000)) == 2

    m.Creditor.query.delete()
    m.Account.query.delete()
    m.LogEntry.query.delete()
    m.LedgerEntry.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    m.CommittedTransfer.query.delete()
    db.session.commit()


def test_process_log_additions(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    m.Account.query.delete()
    m.LogEntry.query.delete()
    m.LedgerEntry.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    m.CommittedTransfer.query.delete()
    db.session.commit()

    app = app_unsafe_session
    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)
    latest_update_id = p.get_account_config(C_ID, D_ID).config_latest_update_id
    p.update_account_config(
        creditor_id=C_ID,
        debtor_id=D_ID,
        is_scheduled_for_deletion=True,
        negligible_amount=1e30,
        allow_unsafe_deletion=False,
        latest_update_id=latest_update_id + 1,
    )
    entries1, _ = p.get_log_entries(C_ID, count=10000)
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'process_log_additions', '--wait=0', '--quit-early'])
    assert result.exit_code == 0
    assert not result.output
    entries2, _ = p.get_log_entries(C_ID, count=10000)
    assert len(entries2) > len(entries1)

    m.Creditor.query.delete()
    m.Account.query.delete()
    m.LogEntry.query.delete()
    m.LedgerEntry.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    m.CommittedTransfer.query.delete()
    db.session.commit()


def test_spawn_worker_processes():
    from swpt_creditors.multiproc_utils import spawn_worker_processes, HANDLED_SIGNALS, try_unblock_signals

    def _quit():
        assert len(HANDLED_SIGNALS) > 0
        try_unblock_signals()

    spawn_worker_processes(
        processes=2,
        target=_quit,
    )


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'consume_messages', '--url=INVALID'])
    assert result.exit_code == 1
