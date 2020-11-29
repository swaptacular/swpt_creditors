import re
import iso8601
from base64 import b16decode
from datetime import date, timedelta
from flask import current_app
from swpt_creditors.extensions import broker, APP_QUEUE_NAME
from swpt_creditors import procedures
from swpt_creditors.models import CT_DIRECT, MIN_INT64, MAX_INT64, TRANSFER_NOTE_MAX_BYTES, \
    TRANSFER_NOTE_FORMAT_REGEX

CONFIG_MAX_BYTES = 2000
RE_TRANSFER_NOTE_FORMAT = re.compile(TRANSFER_NOTE_FORMAT_REGEX)


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_config_signal(
        debtor_id: int,
        creditor_id: int,
        config_ts: str,
        config_seqnum: int,
        negligible_amount: float,
        config: str,
        config_flags: int,
        rejection_code: str,
        ts: str,
        *args, **kwargs) -> None:

    assert rejection_code == '' or len(rejection_code) <= 30 and rejection_code.encode('ascii')
    assert len(config) <= CONFIG_MAX_BYTES and len(config.encode('utf8')) <= CONFIG_MAX_BYTES

    procedures.process_rejected_config_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        config_ts=iso8601.parse_date(config_ts),
        config_seqnum=config_seqnum,
        negligible_amount=negligible_amount,
        config=config,
        config_flags=config_flags,
        rejection_code=rejection_code,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        last_change_ts: str,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        demurrage_rate: float,
        commit_period: int,
        transfer_note_max_bytes: int,
        last_interest_rate_change_ts: str,
        last_transfer_number: int,
        last_transfer_committed_at: str,
        last_config_ts: str,
        last_config_seqnum: int,
        creation_date: str,
        negligible_amount: float,
        config: str,
        config_flags: int,
        status_flags: int,
        ts: str,
        ttl: int,
        account_id: str,
        debtor_info_iri: str,
        debtor_info_content_type: str,
        debtor_info_sha256: str,
        *args, **kwargs) -> None:

    assert 0 <= transfer_note_max_bytes <= TRANSFER_NOTE_MAX_BYTES
    assert len(config) <= CONFIG_MAX_BYTES and len(config.encode('utf8')) <= CONFIG_MAX_BYTES
    assert account_id == '' or len(account_id) <= 100 and account_id.encode('ascii')
    assert len(debtor_info_iri) <= 200
    assert debtor_info_content_type == '' or (
        len(debtor_info_content_type) <= 100 and debtor_info_content_type.encode('ascii'))
    assert debtor_info_sha256 == '' or len(debtor_info_sha256) == 64

    procedures.process_account_update_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=iso8601.parse_date(creation_date).date(),
        last_change_ts=iso8601.parse_date(last_change_ts),
        last_change_seqnum=last_change_seqnum,
        principal=principal,
        interest=interest,
        interest_rate=interest_rate,
        last_interest_rate_change_ts=iso8601.parse_date(last_interest_rate_change_ts),
        transfer_note_max_bytes=transfer_note_max_bytes,
        status_flags=status_flags,
        last_config_ts=iso8601.parse_date(last_config_ts),
        last_config_seqnum=last_config_seqnum,
        negligible_amount=negligible_amount,
        config_flags=config_flags,
        config=config,
        account_id=account_id,
        debtor_info_iri=debtor_info_iri or None,
        debtor_info_content_type=debtor_info_content_type or None,
        debtor_info_sha256=b16decode(debtor_info_sha256, casefold=True) or None,
        last_transfer_number=last_transfer_number,
        last_transfer_committed_at=iso8601.parse_date(last_transfer_committed_at),
        ts=iso8601.parse_date(ts),
        ttl=ttl,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_purge_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: str,
        ts: str,
        *args, **kwargs) -> None:

    procedures.process_account_purge_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=date.fromisoformat(creation_date),
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_number: int,
        creation_date: str,
        coordinator_type: str,
        sender: str,
        recipient: str,
        acquired_amount: int,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: str,
        principal: int,
        ts: str,
        previous_transfer_number: int,
        *args, **kwargs) -> None:

    assert 0 < transfer_number <= MAX_INT64
    assert coordinator_type == '' or len(coordinator_type) <= 30 and coordinator_type.encode('ascii')
    assert sender == '' or len(sender) <= 100 and coordinator_type.encode('ascii')
    assert recipient == '' or len(recipient) <= 100 and coordinator_type.encode('ascii')
    assert acquired_amount != 0
    assert RE_TRANSFER_NOTE_FORMAT.match(transfer_note_format)
    assert len(transfer_note) <= TRANSFER_NOTE_MAX_BYTES
    assert len(transfer_note.encode('utf8')) <= TRANSFER_NOTE_MAX_BYTES
    assert MIN_INT64 <= acquired_amount <= MAX_INT64
    assert MIN_INT64 <= principal <= MAX_INT64
    assert 0 <= previous_transfer_number <= MAX_INT64
    assert previous_transfer_number < transfer_number

    procedures.process_account_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=date.fromisoformat(creation_date),
        transfer_number=transfer_number,
        coordinator_type=coordinator_type,
        sender=sender,
        recipient=recipient,
        acquired_amount=acquired_amount,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        committed_at=iso8601.parse_date(committed_at),
        principal=principal,
        ts=iso8601.parse_date(ts),
        previous_transfer_number=previous_transfer_number,
        retention_interval=timedelta(days=current_app.config['APP_LOG_RETENTION_DAYS']),
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_rejected_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        recipient: str,
        ts: str,
        *args, **kwargs) -> None:

    assert coordinator_type == CT_DIRECT
    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64

    procedures.process_rejected_direct_transfer_signal(
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_prepared_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        recipient: str,
        prepared_at: str,
        demurrage_rate: float,
        deadline: str,
        ts: str,
        *args, **kwargs) -> None:

    assert coordinator_type == CT_DIRECT

    procedures.process_prepared_direct_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        locked_amount=locked_amount,
        recipient=recipient,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_finalized_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        recipient: str,
        status_code: str,
        total_locked_amount: int,
        prepared_at: str,
        ts: str,
        *args, **kwargs) -> None:

    assert coordinator_type == CT_DIRECT
    assert status_code == '' or len(status_code) <= 30 and status_code.encode('ascii')
    assert 0 <= total_locked_amount <= MAX_INT64

    procedures.process_finalized_direct_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        recipient=recipient,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
    )
