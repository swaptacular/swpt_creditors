import iso8601
from datetime import date
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_commit_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_seqnum: int,
        coordinator_type: str,
        other_creditor_id: int,
        committed_at_ts: str,
        committed_amount: int,
        transfer_info: dict,
        account_creation_date: str,
        account_new_principal: int) -> None:
    procedures.process_account_commit_signal(
        debtor_id,
        creditor_id,
        transfer_seqnum,
        coordinator_type,
        other_creditor_id,
        iso8601.parse_date(committed_at_ts),
        committed_amount,
        transfer_info,
        date.fromisoformat(account_creation_date),
        account_new_principal,
    )
