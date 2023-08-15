"""Implement functions that inspect operations susceptible to DoS attacks."""

from base64 import b16encode
from flask import current_app
from swpt_creditors.extensions import redis_store


class ForbiddenOperation(Exception):
    """The operation is forbidden."""


def _calc_accounts_key(creditor_id: int) -> bytes:
    return b"a" + b16encode(
        creditor_id.to_bytes(8, byteorder="big", signed=True)
    )


def _calc_transfers_key(creditor_id: int) -> bytes:
    return b"t" + b16encode(
        creditor_id.to_bytes(8, byteorder="big", signed=True)
    )


def _calc_reconfigs_key(creditor_id: int) -> bytes:
    return b"r" + b16encode(
        creditor_id.to_bytes(8, byteorder="big", signed=True)
    )


def _calc_initiations_key(creditor_id: int) -> bytes:
    return b"i" + b16encode(
        creditor_id.to_bytes(8, byteorder="big", signed=True)
    )


def _default_zero(n) -> int:
    try:
        return int(n)
    except (ValueError, TypeError):  # pragma: no cover
        return 0


def _limit(key: bytes, maximum: int) -> None:
    value = _default_zero(redis_store.get(key))
    if value >= maximum:
        raise ForbiddenOperation


def allow_account_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    key = _calc_accounts_key(creditor_id)
    _limit(key, current_app.config["APP_MAX_CREDITOR_ACCOUNTS"])
    allow_account_reconfig(creditor_id, debtor_id)


def register_account_creation(creditor_id: int, debtor_id: int) -> None:
    increment_account_number(creditor_id, debtor_id)
    register_account_reconfig(creditor_id, debtor_id)


def allow_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    tkey = _calc_transfers_key(creditor_id)
    ikey = _calc_initiations_key(creditor_id)

    with redis_store.pipeline() as p:
        p.get(tkey)
        p.get(ikey)
        transfers_count, initiations_count = [
            _default_zero(n) for n in p.execute()
        ]

    if (
        transfers_count >= current_app.config["APP_MAX_CREDITOR_TRANSFERS"]
        or initiations_count
        >= current_app.config["APP_MAX_CREDITOR_INITIATIONS"]
    ):
        raise ForbiddenOperation


def register_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    tkey = _calc_transfers_key(creditor_id)
    ikey = _calc_initiations_key(creditor_id)
    expiration_seconds = int(
        3600 * current_app.config["APP_CREDITOR_DOS_STATS_CLEAR_HOURS"]
    )

    with redis_store.pipeline() as p:
        p.incr(tkey)
        p.incr(ikey)
        p.expire(ikey, expiration_seconds, nx=True)
        p.execute()


def allow_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""

    key = _calc_reconfigs_key(creditor_id)
    _limit(key, current_app.config["APP_MAX_CREDITOR_RECONFIGS"])


def register_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    key = _calc_reconfigs_key(creditor_id)
    expiration_seconds = int(
        3600 * current_app.config["APP_CREDITOR_DOS_STATS_CLEAR_HOURS"]
    )
    with redis_store.pipeline() as p:
        p.incr(key)
        p.expire(key, expiration_seconds, nx=True)
        p.execute()


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
