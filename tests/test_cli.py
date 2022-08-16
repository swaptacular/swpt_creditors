import logging
from datetime import date, timedelta
from swpt_creditors import procedures as p
from swpt_creditors import models as m

D_ID = -1
C_ID = 4294967296


def _create_new_creditor(creditor_id: int, activate: bool = False):
    creditor = p.reserve_creditor(creditor_id)
    if activate:
        p.activate_creditor(creditor_id, str(creditor.reservation_id))


def test_process_ledger_entries(app, db_session, current_ts):
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


def test_process_log_additions(app, db_session, current_ts):
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


def test_configure_interval(app, db_session, current_ts, caplog):
    caplog.at_level(logging.ERROR)

    ac = m.AgentConfig.query.one_or_none()
    if ac and ac.min_creditor_id == m.MIN_INT64:
        min_creditor_id = m.MIN_INT64 + 1
        max_creditor_id = m.MAX_INT64
    else:
        min_creditor_id = m.MIN_INT64
        max_creditor_id = m.MAX_INT64
    runner = app.test_cli_runner()

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_creditors', 'configure_interval', '--', str(m.MIN_INT64 - 1), '-1'])
    assert result.exit_code != 0
    assert 'not a valid creditor ID' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_creditors', 'configure_interval', '--', '1', str(m.MAX_INT64 + 1)])
    assert result.exit_code != 0
    assert 'not a valid creditor ID' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_creditors', 'configure_interval', '--', '2', '1'])
    assert result.exit_code != 0
    assert 'invalid interval' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_creditors', 'configure_interval', '--', '-1', '1'])
    assert result.exit_code != 0
    assert 'contains 0' in caplog.text

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_creditors', 'configure_interval', '--', '1', str(max_creditor_id)])
    assert result.exit_code == 0
    assert not result.output
    ac = m.AgentConfig.query.one()
    assert ac.min_creditor_id == 1
    assert ac.max_creditor_id == max_creditor_id

    caplog.clear()
    result = runner.invoke(args=[
        'swpt_creditors', 'configure_interval', '--', str(min_creditor_id), '-1'])
    assert result.exit_code == 0
    assert not result.output
    ac = m.AgentConfig.query.one()
    assert ac.min_creditor_id == min_creditor_id
    assert ac.max_creditor_id == -1
