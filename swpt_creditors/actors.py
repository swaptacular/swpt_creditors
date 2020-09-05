import iso8601
from datetime import date, timedelta
from flask import current_app
from swpt_creditors.extensions import broker, APP_QUEUE_NAME
from swpt_creditors import procedures
from swpt_creditors.models import CT_DIRECT


@broker.actor(queue_name=APP_QUEUE_NAME)
def activate_creditor(creditor_id: int) -> None:
    """Activate a creditor."""

    procedures.activate_creditor(creditor_id)


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

    procedures.process_rejected_config_signal(
        debtor_id,
        creditor_id,
        iso8601.parse_date(config_ts),
        config_seqnum,
        negligible_amount,
        config,
        config_flags,
        rejection_code,
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
        *args, **kwargs) -> None:

    procedures.process_account_update_signal(
        debtor_id,
        creditor_id,
        iso8601.parse_date(creation_date).date(),
        iso8601.parse_date(last_change_ts),
        last_change_seqnum,
        principal,
        interest,
        interest_rate,
        iso8601.parse_date(last_interest_rate_change_ts),
        transfer_note_max_bytes,
        status_flags,
        iso8601.parse_date(last_config_ts),
        last_config_seqnum,
        negligible_amount,
        config_flags,
        config,
        account_id,
        debtor_info_iri,
        last_transfer_number,
        iso8601.parse_date(last_transfer_committed_at),
        iso8601.parse_date(ts),
        ttl,
    )


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_purge_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: str,
        ts: str,
        *args, **kwargs) -> None:

    procedures.process_account_purge_signal(
        debtor_id,
        creditor_id,
        date.fromisoformat(creation_date),
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

    procedures.process_account_transfer_signal(
        debtor_id,
        creditor_id,
        date.fromisoformat(creation_date),
        transfer_number,
        coordinator_type,
        sender,
        recipient,
        acquired_amount,
        transfer_note_format,
        transfer_note,
        iso8601.parse_date(committed_at),
        principal,
        iso8601.parse_date(ts),
        previous_transfer_number,
        timedelta(days=current_app.config['APP_TRANSFERS_MIN_RETENTION_DAYS']),
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
    procedures.process_rejected_direct_transfer_signal(
        coordinator_id,
        coordinator_request_id,
        status_code,
        total_locked_amount,
        debtor_id,
        creditor_id,
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
        debtor_id,
        creditor_id,
        transfer_id,
        coordinator_id,
        coordinator_request_id,
        locked_amount,
        recipient,
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
    procedures.process_finalized_direct_transfer_signal(
        debtor_id,
        creditor_id,
        transfer_id,
        coordinator_id,
        coordinator_request_id,
        committed_amount,
        recipient,
        status_code,
        total_locked_amount,
    )
