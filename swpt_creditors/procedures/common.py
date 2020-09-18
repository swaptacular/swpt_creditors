from typing import Callable, Dict, Any
from sqlalchemy.orm import load_only
from swpt_creditors.models import MIN_INT64, MAX_INT64
from . import errors

ACCOUNT_DATA_CONFIG_RELATED_COLUMNS = [
    'creditor_id',
    'debtor_id',
    'creation_date',
    'last_config_ts',
    'last_config_seqnum',
    'negligible_amount',
    'config_flags',
    'allow_unsafe_deletion',
    'is_config_effectual',
    'config_error',
    'config_latest_update_id',
    'config_latest_update_ts',
    'has_server_account',
    'last_heartbeat_ts',
    'info_latest_update_id',
    'info_latest_update_ts',
]

ACCOUNT_DATA_LEDGER_RELATED_COLUMNS = [
    'creditor_id',
    'debtor_id',
    'creation_date',
    'ledger_principal',
    'ledger_last_entry_id',
    'ledger_last_transfer_number',
    'ledger_latest_update_id',
    'ledger_latest_update_ts',
    'principal',
    'interest',
    'interest_rate',
    'last_transfer_number',
    'last_transfer_ts',
    'last_change_ts',
]

ACCOUNT_DATA_INFO_RELATED_COLUMNS = [
    'creditor_id',
    'debtor_id',
    'creation_date',
    'account_id',
    'status_flags',
    'config_flags',
    'config_error',
    'is_config_effectual',
    'has_server_account',
    'interest_rate',
    'last_interest_rate_change_ts',
    'transfer_note_max_bytes',
    'debtor_info_iri',
    'principal',
    'interest',
    'info_latest_update_id',
    'info_latest_update_ts',
]

LOAD_ONLY_CONFIG_RELATED_COLUMNS = load_only(*ACCOUNT_DATA_CONFIG_RELATED_COLUMNS)
LOAD_ONLY_LEDGER_RELATED_COLUMNS = load_only(*ACCOUNT_DATA_LEDGER_RELATED_COLUMNS)
LOAD_ONLY_INFO_RELATED_COLUMNS = load_only(*ACCOUNT_DATA_INFO_RELATED_COLUMNS)


def init(path_builder, type_registry):
    """"Must be called before using any of the functions in the package."""

    global paths, types
    paths = path_builder
    types = type_registry


def get_paths_and_types():
    return paths, types


def allow_update(obj, update_id_field_name: str, update_id: int, update: Dict[str, Any]) -> Callable[[], None]:
    """Return a function that performs the update on `obj`.

    Raises `UpdateConflict` if the update is not allowed. Raises
    `AlreadyUpToDate` when the object is already up-to-date.

    """

    def has_changes():
        return any([getattr(obj, field_name) != value for field_name, value in update.items()])

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
