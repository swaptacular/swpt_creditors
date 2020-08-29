"""Implement functions that inspect operations susceptible to DOS attacks."""


class ForbiddenOperation(Exception):
    """The operation is forbidden."""


def allow_account_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""


def register_account_creation(creditor_id: int, debtor_id: int) -> None:
    increment_account_number(creditor_id, debtor_id)


def allow_direct_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""


def register_direct_transfer_creation(creditor_id: int, debtor_id: int) -> None:
    increment_direct_transfer_number(creditor_id, debtor_id)


def allow_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    """May Raise `ForbiddenOperation`."""


def register_account_reconfig(creditor_id: int, debtor_id: int) -> None:
    pass


def increment_account_number(creditor_id: int, debtor_id: int) -> None:
    pass


def decrement_account_number(creditor_id: int, debtor_id: int) -> None:
    pass


def increment_direct_transfer_number(creditor_id: int, debtor_id: int) -> None:
    pass


def decrement_direct_transfer_number(creditor_id: int, debtor_id: int) -> None:
    pass
