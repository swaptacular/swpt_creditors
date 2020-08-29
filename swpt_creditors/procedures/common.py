from typing import Callable, Dict, Any
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
    'ledger_last_transfer_committed_at_ts',
    'ledger_latest_update_id',
    'ledger_latest_update_ts',
    'principal',
    'interest',
    'interest_rate',
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
    'debtor_info_url',
    'principal',
    'interest',
    'info_latest_update_id',
    'info_latest_update_ts',
]


def init(path_builder, schema_types):
    """"Must be called before using any of the functions in the package."""

    global paths, types
    paths = path_builder
    types = schema_types


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