import pytest
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


@pytest.mark.skip
def test_on_account_transfer_signal(db_session):
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_number=1,
        coordinator_type='direct',
        committed_at='2019-10-01T00:00:00Z',
        acquired_amount=1000,
        transfer_note='{"message": "test"}',
        creation_date='2020-01-02',
        principal=1000,
        previous_transfer_number=0,
        sender='666',
        recipient=str(C_ID),
    )
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_number=1,
        coordinator_type='direct',
        committed_at='2019-10-01T00:00:00Z',
        acquired_amount=1000,
        transfer_note='test',
        creation_date='2020-01-02',
        principal=1000,
        previous_transfer_number=0,
        sender='666',
        recipient=str(C_ID),
    )
