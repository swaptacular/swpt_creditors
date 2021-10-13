import pytest
from datetime import timedelta, date
from swpt_creditors.extensions import db
from swpt_creditors import procedures as p
from swpt_creditors import models as m

D_ID = -1
C_ID = 4294967296


def _create_new_creditor(creditor_id: int, activate: bool = False):
    creditor = p.reserve_creditor(creditor_id)
    if activate:
        p.activate_creditor(creditor_id, creditor.reservation_id)


@pytest.mark.unsafe
def test_scan_creditors(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    db.session.commit()

    _create_new_creditor(C_ID + 1, activate=False)
    _create_new_creditor(C_ID + 2, activate=False)
    _create_new_creditor(C_ID + 3, activate=True)
    _create_new_creditor(C_ID + 4, activate=True)
    _create_new_creditor(C_ID + 5, activate=True)
    _create_new_creditor(C_ID + 6, activate=True)
    m.Creditor.query.filter_by(creditor_id=C_ID + 1).update({
        'created_at': current_ts - timedelta(days=30),
    })
    p.deactivate_creditor(C_ID + 3)
    p.deactivate_creditor(C_ID + 4)
    p.deactivate_creditor(C_ID + 6)
    m.Creditor.query.filter_by(creditor_id=C_ID + 3).update({
        'created_at': current_ts - timedelta(days=3000),
        'deactivation_date': (current_ts - timedelta(days=3000)).date(),
    })
    m.Creditor.query.filter_by(creditor_id=C_ID + 4).update({
        'created_at': current_ts - timedelta(days=3000),
        'deactivation_date': (current_ts - timedelta(days=3000)).date(),
    })
    db.session.commit()
    app = app_unsafe_session
    assert len(m.Creditor.query.all()) == 6

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_creditors', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0

    creditors = m.Creditor.query.all()
    assert len(creditors) == 3
    assert sorted([c.creditor_id for c in creditors]) == [C_ID + 2, C_ID + 5, C_ID + 6]

    m.Creditor.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_scan_accounts(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    m.LedgerEntry.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    db.session.commit()

    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, 2)
    p.create_new_account(C_ID, 3)
    p.create_new_account(C_ID, 4)
    app = app_unsafe_session

    data2 = m.AccountData.query.filter_by(debtor_id=2).one()
    data2.principal = 1000
    data2.ledger_latest_update_ts = current_ts - timedelta(days=60)
    data2.last_config_ts = current_ts - timedelta(days=1000)

    data3 = m.AccountData.query.filter_by(debtor_id=3).one()
    data3.last_transfer_number = 1
    data3.last_transfer_committed_at = current_ts - timedelta(days=1000)

    data4 = m.AccountData.query.filter_by(debtor_id=4).one()
    data4.last_transfer_number = 1
    data4.ledger_pending_transfer_ts = current_ts - timedelta(days=1000)

    db.session.commit()
    assert len(m.LedgerEntry.query.all()) == 0
    assert len(m.PendingLogEntry.query.all()) == 0
    assert len(m.PendingLedgerUpdate.query.all()) == 0

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0

    le = m.LedgerEntry.query.one()
    assert le.creditor_id == C_ID
    assert le.debtor_id == 2
    assert le.entry_id == 1
    assert le.creation_date is None
    assert le.transfer_number is None
    assert le.aquired_amount == 1000
    assert le.principal == 1000
    assert le.added_at >= current_ts

    ple = m.PendingLogEntry.query.filter_by(object_type_hint=m.LogEntry.OTH_ACCOUNT_LEDGER).one()
    assert ple.creditor_id == C_ID
    assert ple.added_at >= current_ts
    assert ple.object_update_id == data2.ledger_latest_update_id
    assert not ple.is_deleted

    ple = m.PendingLogEntry.query.filter_by(object_type='AccountInfo').one()
    assert ple.creditor_id == C_ID
    assert ple.added_at >= current_ts
    assert ple.object_update_id == data2.info_latest_update_id
    assert not ple.is_deleted

    data2 = m.AccountData.query.filter_by(debtor_id=2).one()
    assert data2.ledger_principal == data2.principal == 1000
    assert data2.ledger_last_transfer_number == data2.last_transfer_number == 0
    assert data2.ledger_last_entry_id == 1
    assert data2.ledger_latest_update_ts >= current_ts
    assert data2.config_error == 'CONFIGURATION_IS_NOT_EFFECTUAL'

    plu_list = m.PendingLedgerUpdate.query.all()
    assert len(plu_list) == 2
    plu_list.sort(key=lambda plu: plu.debtor_id)
    assert plu_list[0].debtor_id == 3
    assert plu_list[1].debtor_id == 4

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_accounts', '--hours', '0.00005', '--quit-early'])
    assert result.exit_code == 0
    assert len(m.LedgerEntry.query.all())

    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    m.LedgerEntry.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_scan_log_entries(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    db.session.commit()

    _create_new_creditor(C_ID, activate=True)
    creditor = m.Creditor.query.one()
    creditor.creditor_latest_update_id += 1
    db.session.add(m.LogEntry(
        creditor_id=creditor.creditor_id,
        entry_id=creditor.generate_log_entry_id(),
        object_type='Creditor',
        object_uri='/creditors/1/',
        object_update_id=creditor.creditor_latest_update_id,
        added_at=current_ts,
    ))
    creditor.creditor_latest_update_id += 1
    db.session.add(m.LogEntry(
        creditor_id=creditor.creditor_id,
        entry_id=creditor.generate_log_entry_id(),
        object_type='Creditor',
        object_uri='/creditors/1/',
        object_update_id=creditor.creditor_latest_update_id,
        added_at=current_ts - timedelta(days=1000),
    ))
    db.session.commit()
    assert len(m.LogEntry.query.all()) == 2
    app = app_unsafe_session

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_log_entries', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(m.LogEntry.query.all()) == 1
    le = m.LogEntry.query.one()
    assert le.added_at == current_ts

    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_scan_ledger_entries(app_unsafe_session, current_ts):
    from swpt_creditors.procedures.account_updates import _update_ledger

    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    m.Account.query.delete()
    m.LedgerEntry.query.delete()
    db.session.commit()

    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)
    data = m.AccountData.query.one()
    _update_ledger(data, 1, 1000, 1000, current_ts - timedelta(days=1000))
    _update_ledger(data, 1, 500, 1500, current_ts)
    db.session.commit()
    assert len(m.LedgerEntry.query.all()) == 2
    app = app_unsafe_session

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_ledger_entries', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(m.LedgerEntry.query.all()) == 1
    le = m.LedgerEntry.query.one()
    assert le.added_at == current_ts

    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    m.Account.query.delete()
    m.LedgerEntry.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_scan_committed_transfers(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    m.Account.query.delete()
    m.CommittedTransfer.query.delete()
    db.session.commit()

    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)
    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': date(2020, 1, 2),
        'transfer_number': 1,
        'coordinator_type': 'direct',
        'sender': '666',
        'recipient': str(C_ID),
        'acquired_amount': 100,
        'transfer_note_format': 'json',
        'transfer_note': '{"message": "test"}',
        'committed_at': current_ts - timedelta(days=1000),
        'principal': 1000,
        'ts': current_ts - timedelta(days=1000),
        'previous_transfer_number': 0,
        'retention_interval': timedelta(days=2000),
    }
    p.process_account_transfer_signal(**params)
    params['committed_at'] = current_ts
    params['ts'] = current_ts
    params['transfer_number'] = 2
    p.process_account_transfer_signal(**params)
    assert len(m.CommittedTransfer.query.all()) == 2
    app = app_unsafe_session

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_committed_transfers', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(m.CommittedTransfer.query.all()) == 1
    ct = m.CommittedTransfer.query.one()
    assert ct.transfer_number == 2

    m.Creditor.query.delete()
    m.LogEntry.query.delete()
    m.Account.query.delete()
    m.CommittedTransfer.query.delete()
    db.session.commit()
