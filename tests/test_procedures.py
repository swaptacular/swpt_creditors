import pytest
from datetime import date, timedelta
from uuid import UUID
from swpt_creditors import procedures as p
from swpt_creditors import models
from swpt_creditors.models import Creditor, Account, AccountData, AccountConfig, ConfigureAccountSignal

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
    p.create_new_account(C_ID, D_ID)


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


@pytest.mark.skip
def test_process_pending_account_commits(db_session, setup_account, current_ts):
    ny2019 = date(2019, 1, 1)
    p.process_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_number=1,
        coordinator_type='direct',
        committed_at_ts=current_ts,
        acquired_amount=1000,
        transfer_note='',
        creation_date=ny2019,
        principal=1000,
        previous_transfer_number=0,
        sender='666',
        recipient=str(C_ID),
    )
    assert p.process_pending_account_commits(C_ID, D_ID)


@pytest.mark.skip
def test_process_pending_account_commits_no_creditor(db_session):
    assert p.process_pending_account_commits(C_ID, D_ID)


@pytest.mark.skip
def test_find_legible_pending_account_commits(db_session):
    p.find_legible_pending_account_commits(max_count=10)


@pytest.mark.skip
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


def test_delete_account(db_session, setup_account, current_ts):
    p.delete_account(C_ID, 1234)
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=1,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=current_ts,
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=0.0,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=0,
        debtor_info_url='',
    )
    account = Account.query.one()
    assert not account.config.is_scheduled_for_deletion
    with pytest.raises(p.UnsafeAccountDeletionError):
        p.delete_account(C_ID, D_ID)

    assert AccountConfig.query.one()
    p.change_account_config(C_ID, D_ID, True, 0.0, False)
    p.delete_account(C_ID, D_ID)
    assert AccountConfig.query.one_or_none() is None


@pytest.mark.skip
def test_process_account_update_signal(db_session, creditor, setup_account, current_ts):
    ad = AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    ac = AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert not ad.is_config_effectual
    assert ac.negligible_amount > 1e20
    assert ac.config == ''
    assert ac.config_flags == 0
    last_ts = ad.last_config_ts
    last_seqnum = ad.last_config_seqnum

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=1,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=last_ts,
        last_config_seqnum=last_seqnum,
        creation_date=date(2020, 1, 15),
        negligible_amount=models.DEFAULT_NEGLIGIBLE_AMOUNT,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=0,
        debtor_info_url='',
    )
    ad = AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_ts == ad.last_config_ts
    assert last_seqnum == ad.last_config_seqnum
    assert ad.is_config_effectual

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=2,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=2,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=last_ts,
        last_config_seqnum=last_seqnum,
        creation_date=date(2020, 1, 15),
        negligible_amount=3.0,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=0,
        debtor_info_url='',
    )
    ad = AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_ts == ad.last_config_ts
    assert last_seqnum == ad.last_config_seqnum
    assert not ad.is_config_effectual

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=3,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=2,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=last_ts,
        last_config_seqnum=last_seqnum,
        creation_date=date(2020, 1, 15),
        negligible_amount=models.DEFAULT_NEGLIGIBLE_AMOUNT,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=0,
        debtor_info_url='',
    )
    ad = AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    assert last_ts == ad.last_config_ts
    assert last_seqnum == ad.last_config_seqnum
    assert ad.is_config_effectual

    # Discard orphaned account.
    p.process_account_update_signal(
        debtor_id=1235,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=1,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=current_ts - timedelta(days=5),
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=2.0,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=0,
        debtor_info_url='',
    )
    cas = ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()
    assert cas.negligible_amount > 1e22
    assert cas.is_scheduled_for_deletion
    p.process_account_update_signal(
        debtor_id=1235,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=2,
        principal=1100,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=1,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=current_ts - timedelta(days=5),
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=models.DEFAULT_NEGLIGIBLE_AMOUNT,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        debtor_info_url='',
    )
    assert ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()


def test_process_account_purge_signal(db_session, creditor, setup_account, current_ts):
    data = AccountData.query.one()
    assert data.debtor_id == D_ID
    assert data.creditor_id == C_ID
    assert not data.has_server_account
    assert len(Account.query.all()) == 1
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1000,
        interest=0.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts,
        last_transfer_number=1,
        last_transfer_committed_at_ts=current_ts,
        last_config_ts=current_ts,
        last_config_seqnum=1,
        creation_date=date(2020, 1, 15),
        negligible_amount=0.0,
        status_flags=0,
        ts=current_ts,
        ttl=1000000,
        account_id=str(C_ID),
        config='',
        config_flags=0,
        debtor_info_url='',
    )
    data = AccountData.query.one()
    assert data.has_server_account
    assert len(Account.query.all()) == 1

    # Wrong creation date:
    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2021, 1, 2),
    )
    data = AccountData.query.one()
    assert data.has_server_account
    assert len(Account.query.all()) == 1

    # Wrong creditor_id:
    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=1234,
        creation_date=date(2020, 1, 15),
    )
    data = AccountData.query.one()
    assert data.has_server_account
    assert len(Account.query.all()) == 1

    # Everything is correct:
    p.process_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2020, 1, 15),
    )
    data = AccountData.query.one()
    assert not data.has_server_account
    assert len(AccountData.query.all()) == 1
