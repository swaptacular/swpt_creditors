from datetime import datetime, timezone, timedelta
from swpt_creditors.extensions import db
from swpt_creditors import procedures as p
from swpt_creditors import models as m

D_ID = -1
C_ID = 1


def test_scan_accounts(app_unsafe_session, current_ts):
    m.Creditor.query.delete()
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    db.session.commit()

    current_ts = datetime.now(tz=timezone.utc)
    p.create_new_creditor(C_ID, activate=True)
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
    data3.last_transfer_ts = current_ts - timedelta(days=1000)

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
    assert le.added_at_ts >= current_ts

    ple = m.PendingLogEntry.query.filter_by(object_type='AccountLedger').one()
    assert ple.creditor_id == C_ID
    assert ple.added_at_ts >= current_ts
    assert ple.object_update_id == 2
    assert not ple.is_deleted

    ple = m.PendingLogEntry.query.filter_by(object_type='AccountInfo').one()
    assert ple.creditor_id == C_ID
    assert ple.added_at_ts >= current_ts
    assert ple.object_update_id == 2
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
    m.PendingLogEntry.query.delete()
    m.PendingLedgerUpdate.query.delete()
    db.session.commit()
