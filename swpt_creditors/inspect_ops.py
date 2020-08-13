"""Implement functions that inspect operations susceptible to DOS attacks."""


class ForbiddenAccountCreationError(Exception):
    """The creation of the account is forbidden."""


def allow_account_creation(creditor_id: int, debtor_id: int, accounts_count_limit: int) -> None:
    if accounts_count_limit <= 0:
        raise ForbiddenAccountCreationError()


def register_account_creation(creditor_id: int, debtor_id: int) -> None:
    pass
