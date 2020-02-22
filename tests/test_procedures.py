from datetime import date, timedelta
from uuid import UUID
from swpt_creditors import procedures as p

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')
RECIPIENT_URI = 'https://example.com/creditors/1'


def test_process_pending_account_commits(db_session, current_ts):
    ny2020 = date(2020, 1, 1)
    p.process_account_commit_signal(D_ID, C_ID, 1, 'direct', 666, current_ts, 1000, {}, ny2020, 1000)
    assert p.process_pending_account_commits(C_ID, D_ID)


def test_find_legible_pending_account_commits(db_session):
    p.find_legible_pending_account_commits(max_count=10)


def test_configure_new_account(db_session):
    account_config = p.configure_new_account(C_ID, D_ID)
    assert account_config
    assert 0
