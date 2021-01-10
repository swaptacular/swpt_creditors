from swpt_creditors import actors as a

D_ID = -1
C_ID = 4294967296


def test_on_rejected_config_signal(db_session):
    a.on_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        config_ts='2019-10-01T00:00:00Z',
        config_seqnum=123,
        negligible_amount=100.0,
        config_data='',
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
        transfer_note_format='json',
        transfer_note='{"message": "test"}',
        committed_at='2019-10-01T00:00:00Z',
        principal=1000,
        ts='2000-01-01T00:00:00Z',
        previous_transfer_number=0,
    )


def test_on_account_update_signal(db_session):
    a.on_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts='2019-10-01T00:00:00Z',
        last_change_seqnum=1,
        principal=1000,
        interest=123.0,
        interest_rate=7.5,
        demurrage_rate=-50.0,
        commit_period=100000,
        transfer_note_max_bytes=500,
        last_interest_rate_change_ts='2019-10-01T00:00:00Z',
        last_transfer_number=5,
        last_transfer_committed_at='2019-10-01T00:00:00Z',
        last_config_ts='2019-10-01T00:00:00Z',
        last_config_seqnum=1,
        creation_date='2019-01-01',
        negligible_amount=100.0,
        config_data='',
        config_flags=0,
        ts='2019-10-01T00:00:00Z',
        ttl=10000,
        account_id=str(C_ID),
        debtor_info_iri='http://example.com',
        debtor_info_content_type='text/plain',
        debtor_info_sha256=32 * 'FF',
    )


def test_on_rejected_direct_transfer_signal(db_session):
    a.on_rejected_direct_transfer_signal(
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        status_code='TEST',
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=C_ID,
        ts='2019-10-01T00:00:00Z',
    )


def test_on_prepared_direct_transfer_signal(db_session):
    a.on_prepared_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=1,
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        locked_amount=1000,
        recipient='1111',
        prepared_at='2019-10-01T00:00:00Z',
        demurrage_rate=-50.0,
        deadline='2019-10-01T00:00:00Z',
        ts='2019-10-01T00:00:00Z',
    )


def test_on_finalized_direct_transfer_signal(db_session):
    a.on_finalized_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        committed_amount=100,
        status_code='OK',
        total_locked_amount=0,
        prepared_at='2019-10-01T00:00:00Z',
        ts='2019-10-01T00:00:00Z',
    )
