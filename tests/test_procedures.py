import pytest
import time
from datetime import date, timedelta, datetime, timezone
from uuid import UUID
from swpt_lib.utils import i64_to_u64
from swpt_creditors import procedures as p
from swpt_creditors import models
from swpt_creditors.models import Creditor, Account, AccountData, ConfigureAccountSignal, LogEntry, \
    CommittedTransfer, PendingLedgerUpdate

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')
RECIPIENT_URI = 'https://example.com/creditors/1'


@pytest.fixture(params=[2, None])
def max_count(request):
    return request.param


@pytest.fixture
def creditor(db_session):
    creditor = p.create_new_creditor(C_ID)
    p.activate_creditor(C_ID)
    return creditor


@pytest.fixture
def setup_account(creditor):
    p.create_new_account(C_ID, D_ID)


@pytest.mark.skip
def test_get_creditor(db_session, creditor):
    creditor = p.get_creditor(C_ID)
    assert creditor.creditor_id == C_ID


@pytest.mark.skip
def test_create_new_creditor(db_session):
    creditor = p.create_new_creditor(C_ID)
    assert creditor.creditor_id == C_ID
    assert len(Creditor.query.all()) == 1
    with pytest.raises(p.CreditorExists):
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
        transfer_note_format='',
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
def test_create_account(db_session, creditor):
    with pytest.raises(p.CreditorDoesNotExist):
        p.create_account(666, D_ID)
    created = p.create_account(C_ID, D_ID)
    assert created
    assert AccountConfig.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()
    created = p.create_account(C_ID, D_ID)
    assert not created


@pytest.mark.skip
def test_change_account_config(db_session, setup_account):
    with pytest.raises(p.AccountDoesNotExist):
        p.change_account_config(C_ID, 1234, False, 0.0, False)
    p.change_account_config(C_ID, D_ID, False, 100.0, True)
    config = AccountConfig.query.one()
    assert config.negligible_amount == 100.0
    assert config.is_scheduled_for_deletion


@pytest.mark.skip
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
        debtor_info_iri='',
        transfer_note_max_bytes=500,
    )
    account = Account.query.one()
    assert not account.config.is_scheduled_for_deletion
    with pytest.raises(p.UnsafeAccountDeletion):
        p.delete_account(C_ID, D_ID)

    assert AccountConfig.query.one()
    p.change_account_config(C_ID, D_ID, True, 0.0, False)
    p.delete_account(C_ID, D_ID)
    assert AccountConfig.query.one_or_none() is None


def test_process_account_update_signal(db_session, setup_account):
    AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).update({
        'ledger_principal': 1001,
        'ledger_last_entry_id': 88,
        'ledger_last_transfer_number': 888,
    })

    def get_data():
        return AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()

    ad = get_data()
    assert not ad.is_config_effectual
    assert ad.ledger_principal == 1001
    assert ad.ledger_last_entry_id == 88
    assert ad.negligible_amount == models.DEFAULT_NEGLIGIBLE_AMOUNT
    assert ad.config_flags == models.DEFAULT_CONFIG_FLAGS
    assert ad.last_change_seqnum == 0
    assert ad.config_error is None
    last_ts = ad.last_config_ts
    last_seqnum = ad.last_config_seqnum
    negligible_amount = ad.negligible_amount
    config_flags = ad.config_flags
    last_heartbeat_ts = ad.last_heartbeat_ts
    creation_date = date(2020, 1, 15)

    time.sleep(0.1)
    current_ts = datetime.now(tz=timezone.utc)
    assert last_heartbeat_ts < current_ts

    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': creation_date,
        'last_change_ts': current_ts,
        'last_change_seqnum': 1,
        'principal': 1000,
        'interest': 12.0,
        'interest_rate': 5.0,
        'last_interest_rate_change_ts': current_ts - timedelta(days=1),
        'transfer_note_max_bytes': 500,
        'status_flags': 5,
        'last_config_ts': last_ts,
        'last_config_seqnum': last_seqnum,
        'negligible_amount': negligible_amount,
        'config_flags': config_flags,
        'config': '',
        'account_id': str(C_ID),
        'debtor_info_iri': 'http://example.com',
        'last_transfer_number': 22,
        'last_transfer_committed_at': current_ts - timedelta(days=2),
        'ts': current_ts,
        'ttl': 0,
    }

    p.process_account_update_signal(**params)
    ad = get_data()
    assert last_heartbeat_ts == ad.last_heartbeat_ts
    assert ad.last_change_seqnum == 0
    assert not ad.is_config_effectual
    assert ad.config_error is None

    params['ttl'] = 10000
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_heartbeat_ts > last_heartbeat_ts
    assert ad.is_config_effectual
    assert ad.creation_date == creation_date
    assert ad.last_change_ts == current_ts
    assert ad.last_change_seqnum == 1
    assert ad.principal == 1000
    assert ad.interest == 12.0
    assert ad.interest_rate == 5.0
    assert ad.last_interest_rate_change_ts == current_ts - timedelta(days=1)
    assert ad.transfer_note_max_bytes == 500
    assert ad.status_flags == 5
    assert ad.last_config_ts == last_ts
    assert ad.last_config_seqnum == last_seqnum
    assert ad.account_id == str(C_ID)
    assert ad.debtor_info_iri == 'http://example.com'
    assert ad.last_transfer_number == 22
    assert ad.last_transfer_committed_at_ts == current_ts - timedelta(days=2)
    assert ad.config_error is None

    p.process_account_update_signal(**params)
    assert ad.last_change_seqnum == 1

    p.process_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        config_ts=last_ts,
        config_seqnum=last_seqnum,
        negligible_amount=negligible_amount,
        config='',
        config_flags=config_flags,
        rejection_code='TEST_CONFIG_ERROR',
    )
    ad = get_data()
    assert ad.config_error == 'TEST_CONFIG_ERROR'

    params['last_change_seqnum'] = 2
    params['principal'] = 1100
    params['negligible_amount'] = 3.33
    params['config_flags'] = 77
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_change_seqnum == 2
    assert ad.principal == 1100
    assert ad.negligible_amount == negligible_amount
    assert ad.config_flags == config_flags
    assert not ad.is_config_effectual
    assert ad.config_error == 'TEST_CONFIG_ERROR'

    params['last_change_seqnum'] = 3
    params['negligible_amount'] = negligible_amount
    params['config_flags'] = config_flags
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_change_seqnum == 3
    assert ad.is_config_effectual
    assert ad.config_error is None

    params['creation_date'] = creation_date + timedelta(days=2)
    p.process_account_update_signal(**params)
    ad = get_data()
    assert ad.last_change_seqnum == 3
    assert ad.is_config_effectual
    assert ad.creation_date == creation_date + timedelta(days=2)
    assert ad.config_error is None
    assert ad.ledger_principal == 0
    assert ad.ledger_last_entry_id == 89
    assert ad.ledger_last_transfer_number == 0

    # Discard orphaned account.
    params['debtor_id'] = 1235
    params['last_change_seqnum'] = 1
    params['negligible_amount'] = 2.0
    p.process_account_update_signal(**params)
    cas = ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()
    assert cas.negligible_amount > 1e22
    assert cas.config_flags & AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    params['last_change_seqnum'] = 2
    params['negligible_amount'] = models.DEFAULT_NEGLIGIBLE_AMOUNT
    params['config_flags'] = AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    p.process_account_update_signal(**params)
    assert ConfigureAccountSignal.query.filter_by(creditor_id=C_ID, debtor_id=1235).one()

    assert list(p.get_creditors_with_pending_log_entries()) == [C_ID]
    p.process_pending_log_entries(1235)
    p.process_pending_log_entries(C_ID)
    assert len(models.LogEntry.query.filter_by(object_type='AccountInfo').all()) == 3
    assert len(models.LogEntry.query.filter_by(object_type='AccountLedger').all()) == 1


def test_process_rejected_config_signal(setup_account):
    c = p.get_account_config(C_ID, D_ID)
    assert c.config_error is None
    p.process_pending_log_entries(C_ID)
    ple_count = len(models.LogEntry.query.all())

    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum,
                                     c.negligible_amount, 'UNEXPECTED', c.config_flags, 'TEST_CODE')
    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum,
                                     c.negligible_amount * 1.0001, '', c.config_flags, 'TEST_CODE')
    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum,
                                     c.negligible_amount, '', c.config_flags ^ 1, 'TEST_CODE')
    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum - 1,
                                     c.negligible_amount, '', c.config_flags, 'TEST_CODE')
    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum + 1,
                                     c.negligible_amount, '', c.config_flags, 'TEST_CODE')
    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts + timedelta(seconds=-1), c.last_config_seqnum,
                                     c.negligible_amount, '', c.config_flags, 'TEST_CODE')
    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts + timedelta(seconds=1), c.last_config_seqnum,
                                     c.negligible_amount, '', c.config_flags, 'TEST_CODE')
    c = p.get_account_config(C_ID, D_ID)
    info_latest_update_id = c.info_latest_update_id
    info_latest_update_ts = c.info_latest_update_ts
    assert c.config_error is None
    assert len(models.PendingLogEntry.query.all()) == 0

    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum,
                                     c.negligible_amount, '', c.config_flags, 'TEST_CODE')
    c = p.get_account_config(C_ID, D_ID)
    assert c.config_error == 'TEST_CODE'
    assert c.info_latest_update_id == info_latest_update_id + 1
    assert c.info_latest_update_ts >= info_latest_update_ts

    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == ple_count + 1
    ple = LogEntry.query.filter_by(object_type='AccountInfo', object_update_id=info_latest_update_id + 1).one()
    assert ple.creditor_id == C_ID
    assert '/info' in ple.object_uri

    p.process_rejected_config_signal(D_ID, C_ID, c.last_config_ts, c.last_config_seqnum,
                                     c.negligible_amount, '', c.config_flags, 'TEST_CODE')
    assert len(LogEntry.query.all()) == ple_count + 1


def test_process_account_purge_signal(db_session, creditor, setup_account, current_ts):
    AccountData.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).update({
        AccountData.creation_date: date(2020, 1, 2),
        AccountData.has_server_account: True,
        AccountData.principal: 1000,
        AccountData.interest: 15.0,
    }, synchronize_session=False)
    db_session.commit()
    data = AccountData.query.one()
    assert data.debtor_id == D_ID
    assert data.creditor_id == C_ID
    assert data.has_server_account
    assert data.principal == 1000
    assert data.interest == 15.0

    p.process_account_purge_signal(1111, 2222, date(2020, 1, 2))
    p.process_account_purge_signal(D_ID, C_ID, date(2020, 1, 1))
    data = AccountData.query.one()
    assert data.has_server_account
    assert data.principal == 1000
    assert data.interest == 15.0
    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == 2

    p.process_account_purge_signal(D_ID, C_ID, date(2020, 1, 2))
    data = AccountData.query.one()
    assert not data.has_server_account
    assert data.principal == 0
    assert data.interest == 0.0

    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == 3
    entry = LogEntry.query.filter_by(object_type='AccountInfo').one()
    assert entry.object_uri == f'/creditors/{i64_to_u64(C_ID)}/accounts/{i64_to_u64(D_ID)}/info'
    assert not entry.is_deleted

    p.process_account_purge_signal(D_ID, C_ID, date(2020, 1, 2))
    p.process_pending_log_entries(C_ID)
    assert len(LogEntry.query.all()) == 3


def test_update_account_config(setup_account, current_ts):
    def get_data():
        return AccountData.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).one()

    def get_info_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(LogEntry.query.filter_by(object_type='AccountInfo').all())

    creation_date = current_ts.date()
    data = get_data()
    assert not data.is_config_effectual
    assert not data.is_deletion_safe
    assert not data.has_server_account
    assert get_info_entries_count() == 0

    p.update_account_config(C_ID, D_ID,
                            is_scheduled_for_deletion=True,
                            negligible_amount=1e30,
                            allow_unsafe_deletion=False,
                            latest_update_id=2)

    data = get_data()
    assert not data.is_config_effectual
    assert not data.is_deletion_safe
    assert not data.has_server_account
    assert get_info_entries_count() == 0

    data = get_data()
    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': creation_date,
        'last_change_ts': current_ts,
        'last_change_seqnum': 1,
        'principal': 0,
        'interest': 0.0,
        'interest_rate': 0.0,
        'last_interest_rate_change_ts': models.TS0,
        'transfer_note_max_bytes': 500,
        'status_flags': 0,
        'last_config_ts': data.last_config_ts,
        'last_config_seqnum': data.last_config_seqnum,
        'negligible_amount': data.negligible_amount,
        'config_flags': data.config_flags,
        'config': '',
        'account_id': str(C_ID),
        'debtor_info_iri': 'http://example.com',
        'last_transfer_number': 0,
        'last_transfer_committed_at': models.TS0,
        'ts': current_ts,
        'ttl': 10000,
    }
    p.process_account_update_signal(**params)
    data = get_data()
    assert data.is_config_effectual
    assert not data.is_deletion_safe
    assert data.has_server_account
    assert get_info_entries_count() == 1

    p.process_account_purge_signal(D_ID, C_ID, creation_date)
    data = get_data()
    assert data.is_config_effectual
    assert data.is_deletion_safe
    assert not data.has_server_account
    assert get_info_entries_count() == 2

    p.update_account_config(C_ID, D_ID,
                            is_scheduled_for_deletion=True,
                            negligible_amount=1e30,
                            allow_unsafe_deletion=False,
                            latest_update_id=3)
    data = get_data()
    assert not data.is_config_effectual
    assert not data.is_deletion_safe
    assert not data.has_server_account
    assert get_info_entries_count() == 3


def test_process_account_transfer_signal(db_session, setup_account, current_ts):
    def get_committed_tranfer_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(LogEntry.query.filter_by(object_type='CommittedTransfer').all())

    def has_pending_ledger_update():
        return len(PendingLedgerUpdate.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).all()) > 0

    def delete_pending_ledger_update():
        PendingLedgerUpdate.query.filter_by(creditor_id=C_ID, debtor_id=D_ID).delete()
        db_session.commit()

    assert not has_pending_ledger_update()
    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2020, 1, 2),
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=1000,
        interest=12.0,
        interest_rate=5.0,
        last_interest_rate_change_ts=current_ts - timedelta(days=1),
        transfer_note_max_bytes=500,
        status_flags=5,
        last_config_ts=current_ts,
        last_config_seqnum=0,
        negligible_amount=100.0,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
        config='',
        account_id=str(C_ID),
        debtor_info_iri='http://example.com',
        last_transfer_number=123,
        last_transfer_committed_at=current_ts - timedelta(days=2),
        ts=current_ts,
        ttl=10000,
    )
    assert has_pending_ledger_update()
    delete_pending_ledger_update()

    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': date(2020, 1, 2),
        'transfer_number': 1,
        'coordinator_type': 'direct',
        'sender': '666',
        'recipient': str(C_ID),
        'acquired_amount': 100,
        'transfer_note_format': 'json',
        'transfer_note': '{"message": "test"}',
        'committed_at_ts': current_ts,
        'principal': 1000,
        'ts': current_ts - timedelta(days=6),
        'previous_transfer_number': 0,
        'retention_interval': timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)
    assert len(CommittedTransfer.query.all()) == 0
    assert get_committed_tranfer_entries_count() == 0
    assert not has_pending_ledger_update()

    params['retention_interval'] = timedelta(days=7)
    p.process_account_transfer_signal(**params)
    ct = CommittedTransfer.query.one()
    assert ct.debtor_id == D_ID
    assert ct.creditor_id == C_ID
    assert ct.creation_date == params['creation_date']
    assert ct.transfer_number == 1
    assert ct.sender_id == '666'
    assert ct.recipient_id == str(C_ID)
    assert ct.acquired_amount == 100
    assert ct.transfer_note_format == params['transfer_note_format']
    assert ct.transfer_note == params['transfer_note']
    assert ct.committed_at_ts == current_ts
    assert ct.principal == 1000
    assert ct.previous_transfer_number == 0
    assert get_committed_tranfer_entries_count() == 1
    assert has_pending_ledger_update()
    delete_pending_ledger_update()

    params['retention_interval'] = timedelta(days=7)
    p.process_account_transfer_signal(**params)
    assert len(CommittedTransfer.query.all()) == 1
    assert get_committed_tranfer_entries_count() == 1
    le = LogEntry.query.filter_by(object_type='CommittedTransfer').one()
    assert le.object_uri == f'/creditors/{i64_to_u64(C_ID)}/accounts/{i64_to_u64(D_ID)}/transfers/18263-1'
    assert not le.is_deleted
    assert le.object_update_id is None
    assert not has_pending_ledger_update()

    params['creditor_id'] = 1235
    p.process_account_transfer_signal(**params)
    assert len(CommittedTransfer.query.all()) == 1
    assert get_committed_tranfer_entries_count() == 1


def test_get_pending_ledger_updates(db_session):
    assert p.get_pending_ledger_updates() == []
    assert p.get_pending_ledger_updates(max_count=10) == []


def test_process_pending_ledger_update(setup_account, max_count, current_ts):
    def get_ledger_update_entries_count():
        p.process_pending_log_entries(C_ID)
        return len(LogEntry.query.filter_by(object_type='AccountLedger').all())

    creation_date = date(2020, 1, 2)

    params = {
        'debtor_id': D_ID,
        'creditor_id': C_ID,
        'creation_date': creation_date,
        'transfer_number': 1,
        'coordinator_type': 'direct',
        'sender': '666',
        'recipient': str(C_ID),
        'acquired_amount': 1000,
        'transfer_note_format': 'json',
        'transfer_note': '{"message": "test"}',
        'committed_at_ts': current_ts,
        'principal': 1100,
        'ts': current_ts,
        'previous_transfer_number': 0,
        'retention_interval': timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)

    params['transfer_number'] = 20
    params['previous_transfer_number'] = 1
    params['principal'] = 2100
    p.process_account_transfer_signal(**params)

    params['transfer_number'] = 22
    params['previous_transfer_number'] = 21
    params['principal'] = 4150
    p.process_account_transfer_signal(**params)

    assert get_ledger_update_entries_count() == 0
    assert p.get_pending_ledger_updates() == []

    p.process_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=creation_date,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=models.TS0,
        transfer_note_max_bytes=500,
        status_flags=0,
        last_config_ts=current_ts,
        last_config_seqnum=0,
        negligible_amount=10.0,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
        config='',
        account_id=str(C_ID),
        debtor_info_iri='http://example.com',
        last_transfer_number=0,
        last_transfer_committed_at=models.TS0,
        ts=current_ts,
        ttl=10000,
    )
    assert get_ledger_update_entries_count() == 0
    assert p.get_pending_ledger_updates() == [(C_ID, D_ID)]
    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000, count=1000)) == 0

    assert p.process_pending_ledger_update(2222, D_ID, max_count=max_count)
    assert p.process_pending_ledger_update(C_ID, 1111, max_count=max_count)

    n = 0
    while not p.process_pending_ledger_update(C_ID, D_ID, max_count=max_count):
        x = len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000))
        assert x > n
        n = x

    lue_count = get_ledger_update_entries_count()
    assert lue_count > 0
    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000, count=1000)) == 3
    assert p.get_pending_ledger_updates() == []

    params['transfer_number'] = 21
    params['previous_transfer_number'] = 20
    params['principal'] = 3150
    p.process_account_transfer_signal(**params)

    assert p.get_pending_ledger_updates() == [(C_ID, D_ID)]
    while not p.process_pending_ledger_update(C_ID, D_ID, max_count=max_count):
        pass
    assert get_ledger_update_entries_count() > lue_count
    assert p.get_pending_ledger_updates() == []
    assert len(p.get_account_ledger_entries(C_ID, D_ID, prev=1000, count=1000)) == 6

    log_entry = p.get_creditor_log_entries(C_ID, count=1000)[0][-1]
    assert log_entry.creditor_id == C_ID
    assert log_entry.object_type == 'AccountLedger'
    assert log_entry.object_uri == '/creditors/1/accounts/18446744073709551615/ledger'
    assert log_entry.object_update_id > 2
    assert not log_entry.is_deleted
