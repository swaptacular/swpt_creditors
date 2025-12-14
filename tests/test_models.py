from datetime import datetime, timezone, timedelta
from swpt_creditors import models as m


def test_sibnalbus_burst_count(app):
    from swpt_creditors import models as m

    assert isinstance(m.ConfigureAccountSignal.signalbus_burst_count, int)
    assert isinstance(m.PrepareTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.FinalizeTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.UpdatedLedgerSignal.signalbus_burst_count, int)
    assert isinstance(m.UpdatedPolicySignal.signalbus_burst_count, int)
    assert isinstance(m.UpdatedFlagsSignal.signalbus_burst_count, int)
    assert isinstance(m.RejectedConfigSignal.signalbus_burst_count, int)


def test_account_data(db_session):
    current_ts = datetime.now(tz=timezone.utc)
    ad = m.AccountData(
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_change_ts=current_ts - timedelta(days=366),
        config_flags=m.DEFAULT_CONFIG_FLAGS,
    )

    assert ad.is_scheduled_for_deletion is False
    ad.is_scheduled_for_deletion = True
    assert ad.is_scheduled_for_deletion is True
    ad.is_scheduled_for_deletion = False
    assert ad.is_scheduled_for_deletion is False

    assert ad.ledger_interest == 0
    ad.interest = 10.0
    assert ad.ledger_interest == 10
    ad.principal = 990
    assert ad.ledger_interest == 10
    ad.interest_rate = 12.5
    assert abs(ad.ledger_interest - (1000 * 1.125 - 1000 + 10)) < 2


def test_log_entry(db_session, current_ts):
    le = m.LogEntry(
        creditor_id=1,
        entry_id=2,
        added_at=current_ts,
        object_type="Object",
        object_uri="/object/1",
        object_update_id=1,
        is_deleted=False,
        data={},
    )
    assert le.is_created
    le.object_update_id = 2
    assert not le.is_created
    le.object_update_id = None
    assert le.is_created
    le.is_deleted = True
    assert not le.is_created


def test_account_data_tuple_size(db_session, current_ts):
    from sqlalchemy import text

    db_session.add(
        m.Creditor(creditor_id=1)
    )
    db_session.flush()
    db_session.add(
        m.Account(
            creditor_id=1,
            debtor_id=2,
            latest_update_ts=current_ts,
        )
    )
    db_session.flush()
    db_session.add(
        m.AccountData(
            creditor_id=1,
            debtor_id=2,
            config_error='CONFIGURATION_IS_NOT_EFFECTUAL',
            config_latest_update_ts=current_ts,
            account_id=100 * 'x',
            debtor_info_iri=(
                "https://www.swaptacular.org/debtors/12345678901234567890/"
                "documents/12345678901234567890/public",
            ),
            debtor_info_content_type=(
                "application/vnd.swaptacular.coin-info+json"
            ),
            debtor_info_sha256=32 * b'0',
            info_latest_update_ts=current_ts,
            ledger_pending_transfer_ts=current_ts,
            ledger_latest_update_ts=current_ts,
        )
    )
    db_session.flush()
    tuple_byte_size = db_session.execute(
        text("SELECT pg_column_size(account_data.*) FROM account_data")
    ).scalar()
    toast_tuple_target = 600
    some_extra_bytes = 40
    assert tuple_byte_size + some_extra_bytes <= toast_tuple_target
