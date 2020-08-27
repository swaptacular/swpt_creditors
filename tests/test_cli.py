from datetime import date, timedelta
from swpt_creditors import procedures as p

D_ID = -1
C_ID = 1


def test_process_ledger_entries(app, db_session, current_ts):
    p.create_new_creditor(C_ID, activate=True)
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
        'status_flags': 0,
        'last_config_ts': current_ts,
        'last_config_seqnum': 1,
        'negligible_amount': 0.0,
        'config_flags': 0,
        'config': '',
        'account_id': str(C_ID),
        'debtor_info_url': 'http://example.com',
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
        'transfer_note': '{"message": "test"}',
        'committed_at_ts': current_ts,
        'principal': 200,
        'ts': current_ts,
        'previous_transfer_number': 0,
        'retention_interval': timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)
    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=10000, count=10000)) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'process_ledger_updates'])
    assert not result.output
    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=10000, count=10000)) == 1


def test_process_log_entries(app, db_session, current_ts):
    p.create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)
    p.update_account_config(
        creditor_id=C_ID,
        debtor_id=D_ID,
        is_scheduled_for_deletion=True,
        negligible_amount=1e30,
        allow_unsafe_deletion=False,
        latest_update_id=2,
    )
    entries1, _ = p.get_creditor_log_entries(C_ID, count=10000)
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'process_log_entries'])
    assert not result.output
    entries2, _ = p.get_creditor_log_entries(C_ID, count=10000)
    assert len(entries2) > len(entries1)
