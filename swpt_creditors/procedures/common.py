from typing import Callable, Dict, Any
from datetime import datetime
from sqlalchemy.orm import load_only
from swpt_creditors.extensions import db
from swpt_creditors.models import (
    MIN_INT64,
    MAX_INT64,
    DATE0,
    DEFAULT_CONFIG_FLAGS,
    AccountData,
    UpdatedLedgerSignal,
    UpdatedPolicySignal,
    UpdatedFlagsSignal,
)
from . import errors

ACCOUNT_DATA_CONFIG_RELATED_COLUMNS = [
    AccountData.creditor_id,
    AccountData.debtor_id,
    AccountData.creation_date,
    AccountData.last_config_ts,
    AccountData.last_config_seqnum,
    AccountData.negligible_amount,
    AccountData.config_flags,
    AccountData.config_data,
    AccountData.allow_unsafe_deletion,
    AccountData.is_config_effectual,
    AccountData.config_error,
    AccountData.config_latest_update_id,
    AccountData.config_latest_update_ts,
    AccountData.has_server_account,
    AccountData.last_heartbeat_ts,
    AccountData.info_latest_update_id,
    AccountData.info_latest_update_ts,
]

ACCOUNT_DATA_LEDGER_RELATED_COLUMNS = [
    AccountData.creditor_id,
    AccountData.debtor_id,
    AccountData.creation_date,
    AccountData.account_id,
    AccountData.ledger_principal,
    AccountData.ledger_last_entry_id,
    AccountData.ledger_last_transfer_number,
    AccountData.ledger_latest_update_id,
    AccountData.ledger_latest_update_ts,
    AccountData.principal,
    AccountData.interest,
    AccountData.interest_rate,
    AccountData.last_transfer_number,
    AccountData.last_transfer_committed_at,
    AccountData.last_change_ts,
]

ACCOUNT_DATA_INFO_RELATED_COLUMNS = [
    AccountData.creditor_id,
    AccountData.debtor_id,
    AccountData.creation_date,
    AccountData.account_id,
    AccountData.config_flags,
    AccountData.config_error,
    AccountData.is_config_effectual,
    AccountData.has_server_account,
    AccountData.interest_rate,
    AccountData.last_interest_rate_change_ts,
    AccountData.transfer_note_max_bytes,
    AccountData.debtor_info_iri,
    AccountData.debtor_info_content_type,
    AccountData.debtor_info_sha256,
    AccountData.principal,
    AccountData.interest,
    AccountData.info_latest_update_id,
    AccountData.info_latest_update_ts,
]

LOAD_ONLY_CONFIG_RELATED_COLUMNS = load_only(
    *ACCOUNT_DATA_CONFIG_RELATED_COLUMNS
)
LOAD_ONLY_LEDGER_RELATED_COLUMNS = load_only(
    *ACCOUNT_DATA_LEDGER_RELATED_COLUMNS
)
LOAD_ONLY_INFO_RELATED_COLUMNS = load_only(*ACCOUNT_DATA_INFO_RELATED_COLUMNS)


def init(path_builder, type_registry):
    """ "Must be called before using any of the functions in the package."""

    global paths, types
    paths = path_builder
    types = type_registry


def get_paths_and_types():
    return paths, types


def allow_update(
    obj, update_id_field_name: str, update_id: int, update: Dict[str, Any]
) -> Callable[[], None]:
    """Return a function that performs the update on `obj`.

    Raises `UpdateConflict` if the update is not allowed. Raises
    `AlreadyUpToDate` when the object is already up-to-date.

    """

    def has_changes():
        return any(
            [
                getattr(obj, field_name) != value
                for field_name, value in update.items()
            ]
        )

    def set_values():
        setattr(obj, update_id_field_name, update_id)
        for field_name, value in update.items():
            setattr(obj, field_name, value)
        return True

    latest_update_id = getattr(obj, update_id_field_name)
    if update_id == latest_update_id and not has_changes():
        raise errors.AlreadyUpToDate()

    if update_id != latest_update_id + 1:
        raise errors.UpdateConflict()

    return set_values


def contain_principal_overflow(value: int) -> int:
    if value <= MIN_INT64:  # pragma: no cover
        return -MAX_INT64
    if value > MAX_INT64:  # pragma: no cover
        return MAX_INT64
    return value


def stop_account_trade(
    creditor_id: int,
    debtor_id: int,
    object_update_id: int,
    current_ts: datetime,
) -> None:
    # NOTE: When an account has been deleted, notification messages must be
    # sent to the subsystem that performs automatic circular trades. These
    # are otherwise regular notifications, but they contain the default safe
    # values for all of the fields. (The default values forbid all automatic
    # circular trades for the account.)
    db.session.add(UpdatedLedgerSignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=object_update_id,
        account_id='',
        creation_date=DATE0,
        principal=0,
        last_transfer_number=0,
        ts=current_ts,
    ))
    db.session.add(UpdatedPolicySignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=object_update_id,
        policy_name=None,
        min_principal=MIN_INT64,
        max_principal=MAX_INT64,
        peg_exchange_rate=None,
        peg_debtor_id=None,
        ts=current_ts,
    ))
    db.session.add(UpdatedFlagsSignal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=object_update_id,
        config_flags=DEFAULT_CONFIG_FLAGS,
        ts=current_ts,
    ))
