import iso8601
from datetime import date, timedelta
from swpt_creditors.extensions import broker, APP_QUEUE_NAME
from swpt_creditors import procedures
from flask import current_app


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
        transfer_note,
        iso8601.parse_date(committed_at),
        principal,
        iso8601.parse_date(ts),
        previous_transfer_number,
        timedelta(days=current_app.config['APP_TRANSFERS_MIN_RETENTION_DAYS']),
    )
