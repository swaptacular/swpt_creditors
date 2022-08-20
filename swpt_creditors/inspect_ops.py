"""Implement functions that inspect operations susceptible to DOS attacks."""

from flask import current_app
from swpt_creditors.extensions import redis_store


class ForbiddenOperation(Exception):
    """The operation is forbidden."""


def _calc_accounts_key(creditor_id: int) -> bytes:
    return b'A' + creditor_id.to_bytes(8, byteorder='big', signed=True)


def _calc_transfers_key(creditor_id: int) -> bytes:
    return b'T' + creditor_id.to_bytes(8, byteorder='big', signed=True)


def _calc_reconfigs_key(creditor_id: int) -> bytes:
    return b'R' + creditor_id.to_bytes(8, byteorder='big', signed=True)


def _calc_initiations_key(creditor_id: int) -> bytes:
    return b'I' + creditor_id.to_bytes(8, byteorder='big', signed=True)


def _limit(key: bytes, maximum: int) -> None:
    try:
        value = int(redis_store.get(key))
    except (ValueError, TypeError):  # pragma: no cover
        value = 0

    if value >= maximum:
        raise ForbiddenOperation


def _allow_transfer_initiation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    key = _calc_initiations_key(creditor_id)
    _limit(key, current_app.config['APP_MAX_CREDITOR_INITIATIONS'])


def _register_transfer_initiation(creditor_id: int, debtor_id: int) -> None:
    key = _calc_initiations_key(creditor_id)
    expiration_seconds = int(3600 * current_app.config['APP_CREDITOR_DOS_STATS_CLEAR_HOURS'])
    with redis_store.pipeline() as p:
        p.incrby(key)
        p.expire(key, expiration_seconds, nx=True)


def allow_account_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    key = _calc_accounts_key(creditor_id)
    _limit(key, current_app.config['APP_MAX_CREDITOR_ACCOUNTS'])
    allow_account_reconfig(creditor_id, debtor_id)


def register_account_creation(creditor_id: int, debtor_id: int) -> None:
    increment_account_number(creditor_id, debtor_id)
    register_account_reconfig(creditor_id, debtor_id)


def allow_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    key = _calc_transfers_key(creditor_id)
    _limit(key, current_app.config['APP_MAX_CREDITOR_TRANSFERS'])
    _allow_transfer_initiation(creditor_id, debtor_id)


def register_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    increment_transfer_number(creditor_id, debtor_id)
    _register_transfer_initiation(creditor_id, debtor_id)


def allow_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    key = _calc_reconfigs_key(creditor_id)
    _limit(key, current_app.config['APP_MAX_CREDITOR_RECONFIGS'])


def register_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    key = _calc_reconfigs_key(creditor_id)
    expiration_seconds = int(3600 * current_app.config['APP_CREDITOR_DOS_STATS_CLEAR_HOURS'])
    with redis_store.pipeline() as p:
        p.incrby(key)
        p.expire(key, expiration_seconds, nx=True)


def increment_account_number(creditor_id: int, debtor_id: int) -> None:
    key = _calc_accounts_key(creditor_id)
    redis_store.incr(key)


def decrement_account_number(creditor_id: int, debtor_id: int) -> None:
    key = _calc_accounts_key(creditor_id)
    redis_store.decr(key)


def increment_transfer_number(creditor_id: int, debtor_id: int) -> None:
    key = _calc_transfers_key(creditor_id)
    redis_store.incr(key)


def decrement_transfer_number(creditor_id: int, debtor_id: int) -> None:
    key = _calc_transfers_key(creditor_id)
    redis_store.decr(key)
