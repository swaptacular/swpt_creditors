import pytest
from datetime import date, timedelta
from uuid import UUID
from swpt_creditors import procedures as p

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')
RECIPIENT_URI = 'https://example.com/creditors/1'


@pytest.fixture
def creditor(db_session):
    return p.lock_or_create_creditor(C_ID)


def test_process_pending_account_commits(db_session, creditor, current_ts):
    ny2020 = date(2020, 1, 1)
    p.setup_account(C_ID, D_ID)
    p.process_account_commit_signal(D_ID, C_ID, 1, 'direct', 666, current_ts, 1000, {}, ny2020, 1000)
    assert p.process_pending_account_commits(C_ID, D_ID)


def test_process_pending_account_commits_no_creditor(db_session):
    assert p.process_pending_account_commits(C_ID, D_ID)


def test_find_legible_pending_account_commits(db_session):
    p.find_legible_pending_account_commits(max_count=10)


def test_setup_account(db_session, creditor):
    created = p.setup_account(C_ID, D_ID)
    assert created
    created = p.setup_account(C_ID, D_ID)
    assert not created


def test_process_account_change_signal(db_session, creditor, current_ts):
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_change_ts=current_ts - timedelta(days=5),
        last_config_change_seqnum=1,
        creation_date=date(2020, 1, 1),
        negligible_amount=2.0,
        status=0,
    )
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=2,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_change_ts=current_ts - timedelta(days=5),
        last_config_change_seqnum=1,
        creation_date=date(2020, 1, 1),
        negligible_amount=2.0,
        status=0,
    )
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=2,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_change_ts=current_ts - timedelta(days=5),
        last_config_change_seqnum=1,
        creation_date=date(2020, 1, 1),
        negligible_amount=2.0,
        status=0,
    )
