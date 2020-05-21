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


def test_get_creditor(db_session, creditor):
    creditor = p.get_creditor(C_ID)
    assert creditor.creditor_id == C_ID


def test_create_new_creditor(db_session):
    creditor = p.create_new_creditor(C_ID)
    assert creditor.creditor_id == C_ID
    assert len(Creditor.query.all()) == 1
    with pytest.raises(p.CreditorExistsError):
        p.create_new_creditor(C_ID)
    creditor = p.lock_or_create_creditor(C_ID)
    assert creditor.creditor_id == C_ID
    assert len(Creditor.query.all()) == 1
    creditor = p.lock_or_create_creditor(666)
    assert creditor.creditor_id == 666
    assert len(Creditor.query.all()) == 2


def test_process_pending_account_commits(db_session, setup_account, current_ts):
    ny2019 = date(2019, 1, 1)
    p.process_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_seqnum=1,
        coordinator_type='direct',
        committed_at_ts=current_ts,
        committed_amount=1000,
        transfer_message='',
        transfer_flags=0,
        creation_date=ny2019,
        account_new_principal=1000,
        previous_transfer_seqnum=0,
        system_flags=0,
        sender='666',
        recipient=str(C_ID),
    )
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
        last_config_ts=current_ts,
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=0.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
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
    last_ts = ac.last_ts
    last_seqnum = ac.last_seqnum

    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_ts=last_ts,
        last_config_seqnum=last_seqnum,
        creation_date=date(2020, 1, 15),
        negligible_amount=0.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
    )
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_ts == ac.last_ts
    assert last_seqnum == ac.last_seqnum
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
        last_config_ts=last_ts,
        last_config_seqnum=last_seqnum,
        creation_date=date(2020, 1, 15),
        negligible_amount=3.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
    )
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_ts == ac.last_ts
    assert last_seqnum == ac.last_seqnum
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
        last_config_ts=last_ts,
        last_config_seqnum=last_seqnum,
        creation_date=date(2020, 1, 15),
        negligible_amount=0.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
    )
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_ts == ac.last_ts
    assert last_seqnum == ac.last_seqnum
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
        last_config_ts=current_ts - timedelta(days=5),
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=2.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
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
        last_config_ts=current_ts - timedelta(days=5),
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=1e30,
        status=Account.STATUS_SCHEDULED_FOR_DELETION_FLAG,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
    )
    assert ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()


def test_process_account_purge_signal(db_session, creditor, setup_account, current_ts):
    config = AccountConfig.query.one()
    assert config.debtor_id == D_ID
    assert config.creditor_id == C_ID
    assert not config.has_account
    assert len(Account.query.all()) == 0
    p.process_account_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_ts=current_ts,
        change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_transfer_seqnum=1,
        last_config_ts=current_ts,
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=0.0,
        status=0,
        ts=current_ts,
        ttl=1e30,
        account_identity=str(C_ID),
        config='',
    )
    config = AccountConfig.query.one()
    assert config.has_account
    assert len(Account.query.all()) == 1

    # Wrong creation date:
    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2021, 1, 2),
    )
    config = AccountConfig.query.one()
    assert config.has_account
    assert len(Account.query.all()) == 1

    # Wrong creditor_id:
    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=1234,
        creation_date=date(2020, 1, 15),
    )
    config = AccountConfig.query.one()
    assert config.has_account
    assert len(Account.query.all()) == 1

    # Everything is correct:
    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2020, 1, 15),
    )
    config = AccountConfig.query.one()
    assert not config.has_account
    assert len(Account.query.all()) == 0
