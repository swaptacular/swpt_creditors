import iso8601
from datetime import date
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_committed_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_seqnum: int,
        transfer_epoch: str,
        coordinator_type: str,
        other_creditor_id: int,
        committed_at_ts: str,
        committed_amount: int,
        transfer_info: dict,
        new_account_principal: int) -> None:
    procedures.process_committed_transfer_signal(
        debtor_id,
        creditor_id,
        transfer_seqnum,
        date.fromisoformat(transfer_epoch),
        coordinator_type,
        other_creditor_id,
        iso8601.parse_date(committed_at_ts),
        committed_amount,
        transfer_info,
        new_account_principal,
    )
