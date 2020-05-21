import iso8601
from datetime import date
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_account_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_number: int,
        coordinator_type: str,
        committed_at: str,
        amount: int,
        transfer_message: str,
        transfer_flags: int,
        creation_date: str,
        principal: int,
        previous_transfer_number: int,
        sender: str,
        recipient: str,
        *args, **kwargs) -> None:

    procedures.process_account_transfer_signal(
        debtor_id,
        creditor_id,
        transfer_number,
        coordinator_type,
        iso8601.parse_date(committed_at),
        amount,
        transfer_message,
        transfer_flags,
        date.fromisoformat(creation_date),
        principal,
        previous_transfer_number,
        sender,
        recipient,
    )
