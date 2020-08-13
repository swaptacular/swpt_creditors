"""Implement functions that inspect operations susceptible to DOS attacks."""


class ForbiddenAccountCreationError(Exception):
    """The creation of the account is forbidden."""


def allow_account_creation(creditor_id: int, debtor_id: int, accounts_count_limit: int) -> None:
    if accounts_count_limit <= 0:
        raise ForbiddenAccountCreationError()


def register_new_account(creditor_id: int, debtor_id: int) -> None:
    # NOTE: We must not forget to increment the account number here.
    pass


def configure_existing_account(creditor_id: int, debtor_id: int) -> None:
    pass


def increment_account_number(creditor_id: int, debtor_id: int) -> None:
    pass


def decrement_account_number(creditor_id: int, debtor_id: int) -> None:
    pass
