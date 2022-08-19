from datetime import datetime, date
import pytest
from swpt_pythonlib.rabbitmq import MessageProperties

D_ID = -1
C_ID = 4294967296


@pytest.fixture(scope='function')
def actors():
    from swpt_creditors import actors
    return actors


def test_on_rejected_config_signal(db_session, actors):
    actors._on_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        config_ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        config_seqnum=123,
        negligible_amount=100.0,
        config_data='',
        config_flags=0,
        rejection_code='TEST_REJECTION',
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_on_account_purge_signal(db_session, actors):
    actors._on_account_purge_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date.fromisoformat('2001-01-01'),
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_on_account_transfer_signal(db_session, actors):
    actors._on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date.fromisoformat('2020-01-02'),
        transfer_number=1,
        coordinator_type='direct',
        sender='666',
        recipient=str(C_ID),
        acquired_amount=1000,
        transfer_note_format='json',
        transfer_note='{"message": "test"}',
        committed_at=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        principal=1000,
        ts=datetime.fromisoformat('2000-01-01T00:00:00+00:00'),
        previous_transfer_number=0,
    )


def test_on_account_update_signal(db_session, actors):
    actors._on_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        last_change_seqnum=1,
        principal=1000,
        interest=123.0,
        interest_rate=7.5,
        demurrage_rate=-50.0,
        commit_period=100000,
        transfer_note_max_bytes=500,
        last_interest_rate_change_ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        last_transfer_number=5,
        last_transfer_committed_at=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        last_config_ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        last_config_seqnum=1,
        creation_date=date.fromisoformat('2019-01-01'),
        negligible_amount=100.0,
        config_data='',
        config_flags=0,
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        ttl=10000,
        account_id=str(C_ID),
        debtor_info_iri='http://example.com',
        debtor_info_content_type='text/plain',
        debtor_info_sha256=32 * 'FF',
    )


def test_on_rejected_direct_transfer_signal(db_session, actors):
    actors._on_rejected_direct_transfer_signal(
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        status_code='TEST',
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=C_ID,
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_on_prepared_direct_transfer_signal(db_session, actors):
    actors._on_prepared_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=1,
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        locked_amount=1000,
        recipient='1111',
        prepared_at=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        demurrage_rate=-50.0,
        deadline=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_on_finalized_direct_transfer_signal(db_session, actors):
    actors._on_finalized_direct_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=1,
        committed_amount=100,
        status_code='OK',
        total_locked_amount=0,
        prepared_at=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_activate_creditor_signal(db_session, actors):
    actors._on_activate_creditor_signal(
        creditor_id=C_ID,
        reservation_id='test_id',
        ts=datetime.fromisoformat('2019-10-01T00:00:00+00:00'),
    )


def test_consumer(db_session, actors):
    consumer = actors.SmpConsumer()

    props = MessageProperties(content_type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    assert consumer.process_message(b'{}', props) is False

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    with pytest.raises(RuntimeError, match='The agent is not responsible for this creditor.'):
        consumer.process_message(b'''
        {
          "type": "AccountPurge",
          "debtor_id": 1,
          "creditor_id": 2,
          "creation_date": "2098-12-31",
          "ts": "2099-12-31T00:00:00+00:00"
        }
        ''', props)

    props = MessageProperties(content_type="application/json", type="AccountPurge")
    assert consumer.process_message(b'''
    {
      "type": "AccountPurge",
      "debtor_id": 1,
      "creditor_id": 4294967296,
      "creation_date": "2098-12-31",
      "ts": "2099-12-31T00:00:00+00:00"
    }
    ''', props) is True
