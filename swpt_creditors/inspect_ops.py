"""Implement functions that inspect operations susceptible to DOS attacks."""


class ForbiddenOperationError(Exception):
    """The operation is forbidden."""


def allow_account_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperationError`."""


def register_account_creation(creditor_id: int, debtor_id: int) -> None:
    # NOTE: We must not forget to increment the accounts count here.
    pass


def allow_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperationError`."""


def register_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    pass


def configure_existing_account(creditor_id: int, debtor_id: int) -> None:
    pass


def increment_account_number(creditor_id: int, debtor_id: int) -> None:
    pass


def decrement_account_number(creditor_id: int, debtor_id: int) -> None:
    pass
