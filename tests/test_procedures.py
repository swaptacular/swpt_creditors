import pytest
from datetime import date, timedelta
from uuid import UUID
from swpt_creditors import procedures as p
from swpt_creditors.models import Creditor, Account, AccountConfig, ConfigureAccountSignal

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')
RECIPIENT_URI = 'https://example.com/creditors/1'


@pytest.fixture
def creditor(db_session):
    return p.lock_or_create_creditor(C_ID)


@pytest.fixture
def setup_account(creditor):
    p.create_account(C_ID, D_ID)


def test_create_new_creditor(db_session):
    creditor = p.create_new_creditor(C_ID)
    assert creditor.creditor_id == C_ID
    assert Creditor.query.one()
    with pytest.raises(p.CreditorExistsError):
        p.create_new_creditor(C_ID)
    creditor = p.lock_or_create_creditor(C_ID)
    assert creditor.creditor_id == C_ID
    assert Creditor.query.one()
    creditor = p.lock_or_create_creditor(666)
    assert creditor.creditor_id == 666
    assert len(Creditor.query.all()) == 2


def test_process_pending_account_commits(db_session, setup_account, current_ts):
    ny2020 = date(2020, 1, 1)
    p.process_account_commit_signal(D_ID, C_ID, 1, 'direct', 666, current_ts, 1000, {}, ny2020, 1000)
    assert p.process_pending_account_commits(C_ID, D_ID)


def test_process_pending_account_commits_no_creditor(db_session):
    assert p.process_pending_account_commits(C_ID, D_ID)


def test_find_legible_pending_account_commits(db_session):
    p.find_legible_pending_account_commits(max_count=10)


def test_create_account(db_session, creditor):
    with pytest.raises(p.CreditorDoesNotExistError):
        p.create_account(666, D_ID)
    created = p.create_account(C_ID, D_ID)
    assert created
    assert AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    created = p.create_account(C_ID, D_ID)
    assert not created


def test_change_account_config(db_session, setup_account):
    with pytest.raises(p.AccountDoesNotExistError):
        p.change_account_config(C_ID, 1234, False, 0.0, False)
    p.change_account_config(C_ID, D_ID, False, 100.0, True)
    config = AccountConfig.query.one()
    assert config.negligible_amount == 100.0
    assert config.is_scheduled_for_deletion


def test_try_to_remove_account(db_session, setup_account, current_ts):
    assert p.try_to_remove_account(C_ID, 1234)
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_change_ts=current_ts,
        last_config_change_seqnum=1,
        creation_date=date(2020, 1, 1),
        negligible_amount=0.0,
        status=0,
    )
    account = Account.query.one()
    assert not account.account_config.is_scheduled_for_deletion
    assert not p.try_to_remove_account(C_ID, D_ID)
    assert AccountConfig.query.one()
    p.change_account_config(C_ID, D_ID, True, 0.0, False)
    assert p.try_to_remove_account(C_ID, D_ID)
    assert AccountConfig.query.one_or_none() is None


def test_process_account_change_signal(db_session, creditor, setup_account, current_ts):
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert not ac.is_effectual
    assert ac.negligible_amount == 0.0
    last_change_ts = ac.last_change_ts
    last_change_seqnum = ac.last_change_seqnum

    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_change_ts=last_change_ts,
        last_config_change_seqnum=last_change_seqnum,
        creation_date=date(2020, 1, 1),
        negligible_amount=0.0,
        status=0,
    )
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_change_ts == ac.last_change_ts
    assert last_change_seqnum == ac.last_change_seqnum
    assert ac.is_effectual
    assert ac.negligible_amount == 0.0

    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=2,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=2,
        last_config_change_ts=last_change_ts,
        last_config_change_seqnum=last_change_seqnum,
        creation_date=date(2020, 1, 1),
        negligible_amount=3.0,
        status=0,
    )
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_change_ts == ac.last_change_ts
    assert last_change_seqnum == ac.last_change_seqnum
    assert not ac.is_effectual
    assert ac.negligible_amount == 0.0

    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=3,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=2,
        last_config_change_ts=last_change_ts,  # - timedelta(days=5),
        last_config_change_seqnum=last_change_seqnum,
        creation_date=date(2020, 1, 1),
        negligible_amount=0.0,
        status=0,
    )
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_change_ts == ac.last_change_ts
    assert last_change_seqnum == ac.last_change_seqnum
    assert ac.is_effectual
    assert ac.negligible_amount == 0.0

    # Discard orphaned account.
    p.process_account_change_signal(
        debtor_id=1235,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=1,
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
    cas = ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()
    assert cas.negligible_amount > 1e22
    assert cas.is_scheduled_for_deletion
    p.process_account_change_signal(
        debtor_id=1235,
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
        negligible_amount=1e30,
        status=Account.STATUS_SCHEDULED_FOR_DELETION_FLAG,
    )
    assert ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()
