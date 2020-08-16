from datetime import datetime, timezone
from swpt_creditors import actors as a

D_ID = -1
C_ID = 1


def test_on_rejected_config_signal(db_session):
    a.on_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        config_ts='2019-10-01T00:00:00Z',
        config_seqnum=123,
        negligible_amount=100.0,
        config='',
        config_flags=0,
        rejection_code='TEST_REJECTION',
        ts='2019-10-01T00:00:00Z',
    )


def test_on_account_purge_signal(db_session):
    a.on_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date='2001-01-01',
        ts='2019-10-01T00:00:00Z',
    )


def test_on_account_transfer_signal(db_session):
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date='2020-01-02',
        transfer_number=1,
        coordinator_type='direct',
        sender='666',
        recipient=str(C_ID),
        acquired_amount=1000,
        transfer_note='{"message": "test"}',
        committed_at='2019-10-01T00:00:00Z',
        principal=1000,
        ts='2000-01-01T00:00:00Z',
        previous_transfer_number=0,
    )
