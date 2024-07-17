import sqlalchemy
from unittest.mock import Mock
from datetime import date, timedelta
from swpt_creditors.extensions import db
from swpt_creditors import procedures as p
from swpt_creditors import models as m
from swpt_pythonlib.utils import ShardingRealm

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
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": date(2020, 1, 1),
        "last_change_ts": current_ts,
        "last_change_seqnum": 1,
        "principal": 1000,
        "interest": 0.0,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": current_ts,
        "transfer_note_max_bytes": 500,
        "last_config_ts": current_ts,
        "last_config_seqnum": 1,
        "negligible_amount": 0.0,
        "config_flags": 0,
        "config_data": "",
        "account_id": str(C_ID),
        "debtor_info_iri": "http://example.com",
        "debtor_info_content_type": None,
        "debtor_info_sha256": None,
        "last_transfer_number": 0,
        "last_transfer_committed_at": current_ts,
        "ts": current_ts,
        "ttl": 100000,
    }
    p.process_account_update_signal(**params)

    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": date(2020, 1, 1),
        "transfer_number": 1,
        "coordinator_type": "direct",
        "sender": "666",
        "recipient": str(C_ID),
        "acquired_amount": 200,
        "transfer_note_format": "json",
        "transfer_note": '{"message": "test"}',
        "committed_at": current_ts,
        "principal": 200,
        "ts": current_ts,
        "previous_transfer_number": 0,
        "retention_interval": timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)
    params["transfer_number"] = 2
    params["principal"] = 400
    params["previous_transfer_number"] = 1
    p.process_account_transfer_signal(**params)

    assert (
        len(p.get_account_ledger_entries(C_ID, D_ID, prev=10000, count=10000))
        == 0
    )
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "process_ledger_updates",
            "--burst=1",
            "--quit-early",
            "--wait=0",
        ]
    )
    assert not result.output
    assert (
        len(p.get_account_ledger_entries(C_ID, D_ID, prev=10000, count=10000))
        == 2
    )


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
        config_data="",
        latest_update_id=latest_update_id + 1,
    )
    entries1, _ = p.get_log_entries(C_ID, count=10000)
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "process_log_additions",
            "--wait=0",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert not result.output
    entries2, _ = p.get_log_entries(C_ID, count=10000)
    assert len(entries2) > len(entries1)


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_creditors", "consume_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1


def test_scan_creditors(app, db_session, current_ts):
    _create_new_creditor(C_ID + 1, activate=False)
    _create_new_creditor(C_ID + 2, activate=False)
    _create_new_creditor(C_ID + 3, activate=True)
    _create_new_creditor(C_ID + 4, activate=True)
    _create_new_creditor(C_ID + 5, activate=True)
    _create_new_creditor(C_ID + 6, activate=True)
    m.Creditor.query.filter_by(creditor_id=C_ID + 1).update(
        {
            "created_at": current_ts - timedelta(days=30),
        }
    )
    p.deactivate_creditor(C_ID + 3)
    p.deactivate_creditor(C_ID + 4)
    p.deactivate_creditor(C_ID + 6)
    m.Creditor.query.filter_by(creditor_id=C_ID + 3).update(
        {
            "created_at": current_ts - timedelta(days=3000),
            "deactivation_date": (current_ts - timedelta(days=3000)).date(),
        }
    )
    m.Creditor.query.filter_by(creditor_id=C_ID + 4).update(
        {
            "created_at": current_ts - timedelta(days=3000),
            "deactivation_date": (current_ts - timedelta(days=3000)).date(),
        }
    )
    db.session.commit()
    assert len(m.Creditor.query.all()) == 6

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_creditors",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0

    creditors = m.Creditor.query.all()
    assert len(creditors) == 3
    assert sorted([c.creditor_id for c in creditors]) == [
        C_ID + 2,
        C_ID + 5,
        C_ID + 6,
    ]


def test_delete_parent_creditors(app, db_session, current_ts):
    _create_new_creditor(C_ID, activate=True)
    a2 = p.create_new_account(C_ID, 2)
    a3 = p.create_new_account(C_ID, 3)

    # Create a reference cycle in `AccountExchange`.
    p.update_account_exchange(
        C_ID,
        2,
        policy=None,
        min_principal=0,
        max_principal=0,
        peg_exchange_rate=1.0,
        peg_debtor_id=3,
        latest_update_id=a2.exchange.latest_update_id + 1,
    )
    p.update_account_exchange(
        C_ID,
        3,
        policy=None,
        min_principal=0,
        max_principal=0,
        peg_exchange_rate=1.0,
        peg_debtor_id=2,
        latest_update_id=a3.exchange.latest_update_id + 1,
    )

    db.session.commit()
    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True
    assert len(m.Creditor.query.all()) == 1
    assert len(m.Account.query.all()) == 2
    assert len(m.AccountExchange.query.all()) == 2

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_creditors",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0

    creditors = m.Creditor.query.all()
    assert len(creditors) == 0
    assert len(m.Account.query.all()) == 0
    assert len(m.AccountExchange.query.all()) == 0

    app.config["DELETE_PARENT_SHARD_RECORDS"] = False
    app.config["SHARDING_REALM"] = orig_sharding_realm


def test_scan_accounts(app, db_session, current_ts):
    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, 2)
    p.create_new_account(C_ID, 3)
    p.create_new_account(C_ID, 4)

    data2 = m.AccountData.query.filter_by(debtor_id=2).one()
    data2.principal = 1000
    data2.ledger_latest_update_ts = current_ts - timedelta(days=60)
    data2.last_config_ts = current_ts - timedelta(days=1000)
    data2_creation_date = data2.creation_date

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

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_accounts",
            "--hours",
            "0.000024",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0

    le = m.LedgerEntry.query.one()
    ledger_entry_id = le.entry_id
    assert le.creditor_id == C_ID
    assert le.debtor_id == 2
    assert le.entry_id >= 1
    assert le.creation_date is None
    assert le.transfer_number is None
    assert le.acquired_amount == 1000
    assert le.principal == 1000
    assert le.added_at >= current_ts

    uls = m.UpdatedLedgerSignal.query.one()
    assert uls.creditor_id == C_ID
    assert uls.debtor_id == 2
    assert uls.creation_date == data2_creation_date
    assert uls.principal == 1000
    assert uls.last_transfer_number == 0
    assert isinstance(uls.ts, date)

    data2 = m.AccountData.query.filter_by(debtor_id=2).one()

    ple = m.PendingLogEntry.query.filter_by(
        object_type_hint=m.LogEntry.OTH_ACCOUNT_LEDGER
    ).one()
    assert ple.creditor_id == C_ID
    assert ple.added_at >= current_ts
    assert ple.object_update_id == data2.ledger_latest_update_id
    assert not ple.is_deleted

    ple = m.PendingLogEntry.query.filter_by(object_type="AccountInfo").one()
    assert ple.creditor_id == C_ID
    assert ple.added_at >= current_ts
    assert ple.object_update_id == data2.info_latest_update_id
    assert not ple.is_deleted

    data2 = m.AccountData.query.filter_by(debtor_id=2).one()
    assert data2.ledger_principal == data2.principal == 1000
    assert data2.ledger_last_transfer_number == data2.last_transfer_number == 0
    assert data2.ledger_last_entry_id == ledger_entry_id
    assert data2.ledger_latest_update_ts >= current_ts
    assert data2.config_error == "CONFIGURATION_IS_NOT_EFFECTUAL"

    plu_list = m.PendingLedgerUpdate.query.all()
    assert len(plu_list) == 2
    plu_list.sort(key=lambda plu: plu.debtor_id)
    assert plu_list[0].debtor_id == 3
    assert plu_list[1].debtor_id == 4

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_accounts",
            "--hours",
            "0.00005",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(m.LedgerEntry.query.all())


def test_scan_log_entries(app, db_session, current_ts):
    _create_new_creditor(C_ID, activate=True)
    creditor = m.Creditor.query.one()
    creditor.creditor_latest_update_id += 1
    db.session.add(
        m.LogEntry(
            creditor_id=creditor.creditor_id,
            entry_id=creditor.generate_log_entry_id(),
            object_type="Creditor",
            object_uri="/creditors/1/",
            object_update_id=creditor.creditor_latest_update_id,
            added_at=current_ts,
        )
    )
    creditor.creditor_latest_update_id += 1
    db.session.add(
        m.LogEntry(
            creditor_id=creditor.creditor_id,
            entry_id=creditor.generate_log_entry_id(),
            object_type="Creditor",
            object_uri="/creditors/1/",
            object_update_id=creditor.creditor_latest_update_id,
            added_at=current_ts - timedelta(days=1000),
        )
    )
    db.session.commit()
    assert len(m.LogEntry.query.all()) == 2

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_log_entries",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(m.LogEntry.query.all()) == 1
    le = m.LogEntry.query.one()
    assert le.added_at == current_ts


def test_scan_ledger_entries(app, db_session, current_ts):
    from swpt_creditors.procedures.account_updates import _update_ledger

    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)
    data = m.AccountData.query.one()
    _update_ledger(data, 1, 1000, 1000, current_ts - timedelta(days=1000))
    _update_ledger(data, 1, 500, 1500, current_ts)
    db.session.commit()
    assert len(m.LedgerEntry.query.all()) == 2

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_ledger_entries",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(m.LedgerEntry.query.all()) == 1
    le = m.LedgerEntry.query.one()
    assert le.added_at == current_ts


def test_scan_committed_transfers(app, db_session, current_ts):
    _create_new_creditor(C_ID, activate=True)
    p.create_new_account(C_ID, D_ID)
    params = {
        "debtor_id": D_ID,
        "creditor_id": C_ID,
        "creation_date": date(2020, 1, 2),
        "transfer_number": 1,
        "coordinator_type": "direct",
        "sender": "666",
        "recipient": str(C_ID),
        "acquired_amount": 100,
        "transfer_note_format": "json",
        "transfer_note": '{"message": "test"}',
        "committed_at": current_ts - timedelta(days=1000),
        "principal": 1000,
        "ts": current_ts - timedelta(days=1000),
        "previous_transfer_number": 0,
        "retention_interval": timedelta(days=2000),
    }
    p.process_account_transfer_signal(**params)
    params["committed_at"] = current_ts
    params["ts"] = current_ts
    params["transfer_number"] = 2
    p.process_account_transfer_signal(**params)
    assert len(m.CommittedTransfer.query.all()) == 2

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "scan_committed_transfers",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(m.CommittedTransfer.query.all()) == 1
    ct = m.CommittedTransfer.query.one()
    assert ct.transfer_number == 2


def test_flush_messages(mocker, app, db_session):
    send_signalbus_message = Mock()
    mocker.patch(
        "swpt_creditors.models.FinalizeTransferSignal.send_signalbus_message",
        new_callable=send_signalbus_message,
    )
    fts = m.FinalizeTransferSignal(
        creditor_id=0x0000010000000000,
        debtor_id=D_ID,
        transfer_id=666,
        coordinator_id=C_ID,
        coordinator_request_id=777,
        committed_amount=0,
        transfer_note_format="",
        transfer_note="",
    )
    db.session.add(fts)
    db.session.commit()
    assert len(m.FinalizeTransferSignal.query.all()) == 1
    db.session.commit()

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_creditors",
            "flush_messages",
            "FinalizeTransferSignal",
            "--wait",
            "0.1",
            "--quit-early",
        ]
    )
    assert result.exit_code == 1
    send_signalbus_message.assert_called_once()
    assert len(m.FinalizeTransferSignal.query.all()) == 0
