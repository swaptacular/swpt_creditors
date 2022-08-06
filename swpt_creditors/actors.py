import logging
import json
from datetime import datetime, date, timedelta
from base64 import b16decode
from marshmallow import ValidationError
from flask import current_app
import swpt_pythonlib.protocol_schemas as ps
from swpt_pythonlib import rabbitmq
from swpt_creditors import procedures
from swpt_creditors.models import CT_DIRECT


def _on_rejected_config_signal(
        debtor_id: int,
        creditor_id: int,
        config_ts: datetime,
        config_seqnum: int,
        negligible_amount: float,
        config_data: str,
        config_flags: int,
        rejection_code: str,
        ts: datetime,
        *args, **kwargs) -> None:

    procedures.process_rejected_config_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        config_ts=config_ts,
        config_seqnum=config_seqnum,
        negligible_amount=negligible_amount,
        config_data=config_data,
        config_flags=config_flags,
        rejection_code=rejection_code,
    )


def _on_account_update_signal(
        debtor_id: int,
        creditor_id: int,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        demurrage_rate: float,
        commit_period: int,
        transfer_note_max_bytes: int,
        last_interest_rate_change_ts: datetime,
        last_transfer_number: int,
        last_transfer_committed_at: datetime,
        last_config_ts: datetime,
        last_config_seqnum: int,
        creation_date: date,
        negligible_amount: float,
        config_data: str,
        config_flags: int,
        ts: datetime,
        ttl: int,
        account_id: str,
        debtor_info_iri: str,
        debtor_info_content_type: str,
        debtor_info_sha256: str,
        *args, **kwargs) -> None:

    procedures.process_account_update_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        last_change_ts=last_change_ts,
        last_change_seqnum=last_change_seqnum,
        principal=principal,
        interest=interest,
        interest_rate=interest_rate,
        last_interest_rate_change_ts=last_interest_rate_change_ts,
        transfer_note_max_bytes=transfer_note_max_bytes,
        last_config_ts=last_config_ts,
        last_config_seqnum=last_config_seqnum,
        negligible_amount=negligible_amount,
        config_flags=config_flags,
        config_data=config_data,
        account_id=account_id,
        debtor_info_iri=debtor_info_iri or None,
        debtor_info_content_type=debtor_info_content_type or None,
        debtor_info_sha256=b16decode(debtor_info_sha256, casefold=True) or None,
        last_transfer_number=last_transfer_number,
        last_transfer_committed_at=last_transfer_committed_at,
        ts=ts,
        ttl=ttl,
    )


def _on_account_purge_signal(
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
        ts: str,
        *args, **kwargs) -> None:

    procedures.process_account_purge_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
    )


def _on_account_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_number: int,
        creation_date: date,
        coordinator_type: str,
        sender: str,
        recipient: str,
        acquired_amount: int,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: datetime,
        principal: int,
        ts: datetime,
        previous_transfer_number: int,
        *args, **kwargs) -> None:

    procedures.process_account_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        transfer_number=transfer_number,
        coordinator_type=coordinator_type,
        sender=sender,
        recipient=recipient,
        acquired_amount=acquired_amount,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        committed_at=committed_at,
        principal=principal,
        ts=ts,
        previous_transfer_number=previous_transfer_number,
        retention_interval=timedelta(days=current_app.config['APP_LOG_RETENTION_DAYS']),
    )


def _on_rejected_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        total_locked_amount: int,
        ts: datetime,
        *args, **kwargs) -> None:

    if coordinator_type != CT_DIRECT:
        _LOGGER.error('Unexpected coordinator type: "%s"', coordinator_type)
        return

    procedures.process_rejected_direct_transfer_signal(
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )


def _on_prepared_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        recipient: str,
        prepared_at: datetime,
        demurrage_rate: float,
        deadline: datetime,
        ts: datetime,
        *args, **kwargs) -> None:

    if coordinator_type != CT_DIRECT:
        _LOGGER.error('Unexpected coordinator type: "%s"', coordinator_type)
        return

    procedures.process_prepared_direct_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        locked_amount=locked_amount,
        recipient=recipient,
    )


def _on_finalized_direct_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        status_code: str,
        total_locked_amount: int,
        prepared_at: datetime,
        ts: datetime,
        *args, **kwargs) -> None:

    if coordinator_type != CT_DIRECT:
        _LOGGER.error('Unexpected coordinator type: "%s"', coordinator_type)
        return

    procedures.process_finalized_direct_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
    )


_MESSAGE_TYPES = {
    'RejectedConfig': (ps.RejectedConfigMessageSchema(), _on_rejected_config_signal),
    'AccountUpdate': (ps.AccountUpdateMessageSchema(), _on_account_update_signal),
    'AccountPurge': (ps.AccountPurgeMessageSchema(), _on_account_purge_signal),
    'AccountTransfer': (ps.AccountTransferMessageSchema(), _on_account_transfer_signal),
    'RejectedTransfer': (ps.RejectedTransferMessageSchema(), _on_rejected_direct_transfer_signal),
    'PreparedTransfer': (ps.PreparedTransferMessageSchema(), _on_prepared_direct_transfer_signal),
    'FinalizedTransfer': (ps.FinalizedTransferMessageSchema(), _on_finalized_direct_transfer_signal),
}

_LOGGER = logging.getLogger(__name__)


TerminatedConsumtion = rabbitmq.TerminatedConsumtion


class SmpConsumer(rabbitmq.Consumer):
    """Passes messages to proper handlers (actors)."""

    def process_message(self, body, properties):
        try:
            content_type = properties.content_type
        except AttributeError:
            _LOGGER.error('Missing message content type header')
            return False

        if content_type != 'application/json':
            _LOGGER.error('Unknown message content type: "%s"', content_type)
            return False

        try:
            massage_type = properties.type
        except AttributeError:
            _LOGGER.error('Missing message type header')
            return False

        try:
            schema, actor = _MESSAGE_TYPES[massage_type]
        except KeyError:
            _LOGGER.error('Unknown message type: "%s"', massage_type)
            return False

        try:
            obj = json.loads(body.decode('utf8'))
        except (UnicodeError, json.JSONDecodeError):
            _LOGGER.error('The message does not contain a valid JSON document.')
            return False

        try:
            message_content = schema.load(obj)
        except ValidationError as e:
            _LOGGER.error('Message validation error: %s', str(e))
            return False

        actor(**message_content)
        return True
