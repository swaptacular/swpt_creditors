class UpdateConflict(Exception):
    """A conflict occurred while trying to update a resource."""


class AlreadyUpToDate(Exception):
    """Trying to update a resource which is already up-to-date."""


class CreditorDoesNotExist(Exception):
    """The creditor does not exist."""


class CreditorExists(Exception):
    """The same creditor record already exists."""


class AccountDoesNotExist(Exception):
    """The account does not exist."""


class PegDoesNotExist(Exception):
    """The peg account does not exist."""


class TransferDoesNotExist(Exception):
    """The transfer does not exist."""


class ForbiddenTransferCancellation(Exception):
    """The transfer can not be canceled."""


class AccountExists(Exception):
    """The same account record already exists."""


class UnsafeAccountDeletion(Exception):
    """Unauthorized unsafe deletion of an account."""


class ForbiddenPegDeletion(Exception):
    """Can not delete an account that acts as a currency peg."""


class InvalidExchangePolicy(Exception):
    """Invalid exchange policy."""


class DebtorNameConflict(Exception):
    """Another account with this debtorName already exist."""
