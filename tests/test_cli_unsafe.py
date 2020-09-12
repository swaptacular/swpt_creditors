from datetime import datetime, timezone, timedelta
from swpt_creditors.extensions import db
from swpt_creditors import procedures as p
from swpt_creditors import models as m

D_ID = -1
C_ID = 1


def test_scan_accounts(app_unsafe_session):
    m.Creditor.query.delete()
    m.PendingLogEntry.query.delete()
    db.session.commit()

    current_ts = datetime.now(tz=timezone.utc)
    p.create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)

    app = app_unsafe_session
    data = m.AccountData.query.one()
    data.principal = 1000
    data.ledger_latest_update_ts = current_ts - timedelta(days=60)
    db.session.commit()
    assert len(m.LedgerEntry.query.all()) == 0
    assert len(m.PendingLogEntry.query.all()) == 0

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0

    le = m.LedgerEntry.query.one()
    assert le.creditor_id == C_ID
    assert le.debtor_id == D_ID
    assert le.entry_id == 1
    assert le.creation_date is None
    assert le.transfer_number is None
    assert le.aquired_amount == 1000
    assert le.principal == 1000
    assert le.added_at_ts >= current_ts

    ple = m.PendingLogEntry.query.one()
    assert ple.creditor_id == C_ID
    assert ple.added_at_ts >= current_ts
    assert ple.object_type == 'AccountLedger'
    assert ple.object_update_id == 2
    assert not ple.is_deleted

    data = m.AccountData.query.one()
    assert data.ledger_principal == data.principal == 1000
    assert data.ledger_last_transfer_number == data.last_transfer_number == 0
    assert data.ledger_last_entry_id == 1
    assert data.ledger_latest_update_ts >= current_ts

    db.engine.execute('ANALYZE account')
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_creditors', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(m.LedgerEntry.query.all())

    m.Creditor.query.delete()
    m.PendingLogEntry.query.delete()
    db.session.commit()
