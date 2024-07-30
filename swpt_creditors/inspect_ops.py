"""Implement functions that inspect operations susceptible to DoS attacks."""

import math
from flask import current_app
from swpt_creditors import procedures


class ForbiddenOperation(Exception):
    """The operation is forbidden."""


def allow_account_creation(creditor_id: int, debtor_id: int) -> None:
    if not procedures.is_account_creation_allowed(
            creditor_id,
            current_app.config["APP_MAX_CREDITOR_ACCOUNTS"],
            current_app.config["APP_MAX_CREDITOR_RECONFIGS"],
    ):
        raise ForbiddenOperation


def register_account_creation(creditor_id: int, debtor_id: int) -> None:
    procedures.register_account_creation(
        creditor_id,
        math.ceil(current_app.config["APP_CREDITOR_DOS_STATS_CLEAR_HOURS"]),
    )


def allow_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    if not procedures.is_transfer_creation_allowed(
            creditor_id,
            current_app.config["APP_MAX_CREDITOR_TRANSFERS"],
            current_app.config["APP_MAX_CREDITOR_INITIATIONS"],
    ):
        raise ForbiddenOperation


def register_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    procedures.register_transfer_creation(
        creditor_id,
        math.ceil(current_app.config["APP_CREDITOR_DOS_STATS_CLEAR_HOURS"]),
    )


def allow_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    if not procedures.is_account_reconfig_allowed(
            creditor_id,
            current_app.config["APP_MAX_CREDITOR_RECONFIGS"],
    ):
        raise ForbiddenOperation


def register_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    procedures.register_account_reconfig(
        creditor_id,
        math.ceil(current_app.config["APP_CREDITOR_DOS_STATS_CLEAR_HOURS"]),
    )


def increment_account_number(creditor_id: int, debtor_id: int) -> None:
    procedures.increment_account_number(creditor_id)


def decrement_account_number(creditor_id: int, debtor_id: int) -> None:
    procedures.decrement_account_number(creditor_id)


def increment_transfer_number(creditor_id: int, debtor_id: int) -> None:
    procedures.increment_transfer_number(creditor_id)


def decrement_transfer_number(creditor_id: int, debtor_id: int) -> None:
    procedures.decrement_transfer_number(creditor_id)
