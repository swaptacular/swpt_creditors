from functools import partial
from typing import Tuple
from urllib.parse import urlparse, urljoin
from datetime import date, timedelta
from werkzeug.routing import NotFound, RequestRedirect, MethodNotAllowed
from flask import current_app, redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib.endpoints import get_server_name, get_url_scheme
from swpt_lib.utils import i64_to_u64, u64_to_i64
from swpt_lib.swpt_uris import parse_debtor_uri, parse_account_uri, make_debtor_uri
from swpt_creditors.models import MAX_INT64, DATE0
from swpt_creditors.schemas import (
    CreditorCreationRequestSchema, CreditorSchema, DebtorIdentitySchema, TransferListSchema,
    AccountSchema, AccountConfigSchema, CommittedTransferSchema, LedgerEntriesPageSchema,
    WalletSchema, ObjectReferencesPageSchema, PaginationParametersSchema, LogEntriesPageSchema,
    TransferCreationRequestSchema, TransferSchema, TransferCancelationRequestSchema,
    AccountDisplaySchema, AccountExchangeSchema, AccountIdentitySchema, AccountKnowledgeSchema,
    AccountLedgerSchema, AccountInfoSchema, AccountListSchema, LogPaginationParamsSchema,
    AccountsPaginationParamsSchema, LedgerEntriesPaginationParamsSchema,
)
from swpt_creditors.specs import DID, CID, TID, TRANSFER_UUID
from swpt_creditors import specs
from swpt_creditors import procedures
from swpt_creditors import inspect_ops


def _make_transfer_slug(creation_date: date, transfer_number: int) -> str:
    epoch = (creation_date - DATE0).days
    return f'{epoch}-{transfer_number}'


def _parse_transfer_slug(slug) -> Tuple[date, int]:
    epoch, transfer_number = slug.split('-', maxsplit=1)
    epoch = int(epoch)
    transfer_number = int(transfer_number)

    try:
        creation_date = DATE0 + timedelta(days=epoch)
    except OverflowError:
        raise ValueError from None

    if not 1 <= transfer_number <= MAX_INT64:
        raise ValueError

    return creation_date, transfer_number


def _build_committed_transfer_path(creditorId: int, debtorId: int, creationDate: date, transferNumber: int) -> str:
    return url_for(
        'transfers.CommittedTransferEndpoint',
        creditorId=creditorId,
        debtorId=debtorId,
        transferId=_make_transfer_slug(creationDate, transferNumber),
        _external=False,
    )


def _url_for(name):
    return staticmethod(partial(url_for, name, _external=False))


def _parse_peg_account_uri(creditor_id: int, base_url: str, uri: str) -> int:
    Error = procedures.PegAccountDoesNotExistError

    try:
        scheme, netloc, path, *rest = urlparse(urljoin(base_url, uri))
    except ValueError:
        raise Error()

    if any(rest) or (scheme and scheme != get_url_scheme()) or (netloc and netloc != get_server_name()):
        raise Error()

    try:
        endpoint, params = current_app.url_map.bind('localhost').match(path)
    except (NotFound, RequestRedirect, MethodNotAllowed):
        raise Error()

    if endpoint != 'accounts.AccountEndpoint' or params['creditorId'] != creditor_id:
        raise Error()

    return params['debtorId']


class path_builder:
    creditor = _url_for('creditors.CreditorEndpoint')
    wallet = _url_for('creditors.WalletEndpoint')
    log_entries = _url_for('creditors.LogEntriesEndpoint')
    account_list = _url_for('creditors.AccountListEndpoint')
    transfer_list = _url_for('creditors.TransferListEndpoint')
    account = _url_for('accounts.AccountEndpoint')
    account_info = _url_for('accounts.AccountInfoEndpoint')
    account_ledger = _url_for('accounts.AccountLedgerEndpoint')
    account_display = _url_for('accounts.AccountDisplayEndpoint')
    account_exchange = _url_for('accounts.AccountExchangeEndpoint')
    account_knowledge = _url_for('accounts.AccountKnowledgeEndpoint')
    account_config = _url_for('accounts.AccountConfigEndpoint')
    account_ledger_entries = _url_for('accounts.AccountLedgerEntriesEndpoint')
    accounts = _url_for('accounts.AccountsEndpoint')
    account_lookup = _url_for('accounts.AccountLookupEndpoint')
    debtor_lookup = _url_for('accounts.DebtorLookupEndpoint')
    transfer = _url_for('transfers.TransferEndpoint')
    transfers = _url_for('transfers.TransfersEndpoint')
    committed_transfer = _build_committed_transfer_path


class schema_types:
    creditor = 'Creditor'
    account = 'Account'
    account_knowledge = 'AccountKnowledge'
    account_exchange = 'AccountExchange'
    account_display = 'AccountDisplay'
    account_config = 'AccountConfig'
    account_info = 'AccountInfo'
    account_ledger = 'AccountLedger'
    account_list = 'AccountList'
    committed_transfer = 'CommittedTransfer'


CONTEXT = {'paths': path_builder}
procedures.init(path_builder, schema_types)


creditors_api = Blueprint(
    'creditors',
    __name__,
    url_prefix='/creditors',
    description="Get information about creditors, create new creditors.",
)


@creditors_api.route('/<i64:creditorId>/', parameters=[CID])
class CreditorEndpoint(MethodView):
    @creditors_api.response(CreditorSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getCreditor')
    def get(self, creditorId):
        """Return a creditor."""

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(403)
        return creditor

    @creditors_api.arguments(CreditorCreationRequestSchema)
    @creditors_api.response(CreditorSchema(context=CONTEXT), code=202)
    @creditors_api.doc(operationId='createCreditor',
                       responses={409: specs.CONFLICTING_CREDITOR})
    def post(self, creditor_creation_request, creditorId):
        """Try to create a new creditor. Requires special privileges.

        ---
        Must fail if the creditor already exists.

        """

        try:
            creditor = procedures.create_new_creditor(creditorId, activate=creditor_creation_request['activate'])
        except procedures.CreditorExistsError:
            abort(409)
        return creditor

    @creditors_api.arguments(CreditorSchema)
    @creditors_api.response(CreditorSchema(context=CONTEXT))
    @creditors_api.doc(operationId='updateCreditor',
                       responses={409: specs.UPDATE_CONFLICT})
    def patch(self, creditor, creditorId):
        """Update a creditor.

        **Note:** This is an idempotent operation.

        """

        try:
            creditor = procedures.update_creditor(creditorId, latest_update_id=creditor['latest_update_id'])
        except procedures.CreditorDoesNotExistError:
            abort(403)
        except procedures.UpdateConflictError:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        return creditor


@creditors_api.route('/<i64:creditorId>/wallet', parameters=[CID])
class WalletEndpoint(MethodView):
    @creditors_api.response(WalletSchema(context=CONTEXT))
    @creditors_api.doc(operationId='getWallet')
    def get(self, creditorId):
        """Return creditor's wallet.

        The creditor's wallet "contains" all creditor's accounts,
        pending transfers, and recent events (the log). In short: it
        is the gateway to all objects and operations in the API.

        """

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor


@creditors_api.route('/<i64:creditorId>/log', parameters=[CID])
class LogEntriesEndpoint(MethodView):
    @creditors_api.arguments(LogPaginationParamsSchema, location='query')
    @creditors_api.response(LogEntriesPageSchema(context=CONTEXT), example=specs.LOG_ENTRIES_EXAMPLE)
    @creditors_api.doc(operationId='getLogPage')
    def get(self, params, creditorId):
        """Return a collection of creditor's recent log entries.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains recent log entries. The
        returned fragment, and all the subsequent fragments, will be
        sorted in chronological order (smaller `entryId`s go
        first).

        """

        n = current_app.config['APP_LOG_ENTRIES_PER_PAGE']
        try:
            log_entries, last_log_entry_id = procedures.get_creditor_log_entries(
                creditorId,
                count=n,
                prev=params['prev'],
            )
        except procedures.CreditorDoesNotExistError:
            abort(404)

        if len(log_entries) < n:
            # The last page does not have a 'next' link.
            return {
                'uri': request.full_path,
                'items': log_entries,
                'forthcoming': f'?prev={last_log_entry_id}',
            }

        return {
            'uri': request.full_path,
            'items': log_entries,
            'next': f'?prev={log_entries[-1].entry_id}',
        }


@creditors_api.route('/<i64:creditorId>/account-list', parameters=[CID])
class AccountListEndpoint(MethodView):
    @creditors_api.response(AccountListSchema(context=CONTEXT), example=specs.ACCOUNT_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getAccountList')
    def get(self, creditorId):
        """Return a paginated list of links to all accounts belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor


@creditors_api.route('/<i64:creditorId>/transfer-list', parameters=[CID])
class TransferListEndpoint(MethodView):
    @creditors_api.response(TransferListSchema(context=CONTEXT), example=specs.TRANSFER_LIST_EXAMPLE)
    @creditors_api.doc(operationId='getTransferList')
    def get(self, creditorId):
        """Return a paginated list of links to all transfers belonging to a
        creditor.

        The paginated list will not be sorted in any particular order.

        """

        creditor = procedures.get_creditor(creditorId)
        if creditor is None:
            abort(404)
        return creditor


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="Create, view, update, and delete accounts, view account's transaction history.",
)


@accounts_api.route('/<i64:creditorId>/account-lookup', parameters=[CID])
class AccountLookupEndpoint(MethodView):
    @accounts_api.arguments(AccountIdentitySchema, example=specs.ACCOUNT_IDENTITY_EXAMPLE)
    @accounts_api.response(DebtorIdentitySchema)
    @accounts_api.doc(operationId='accountLookup')
    def post(self, account_identity, creditorId):
        """Given an account identity, find the debtor's identity.

        This can be useful, for example, when the creditor wants to
        send money to some other creditor's account, but he does not
        know if he already has an account with the same debtor (that
        is: the debtor of the other creditor's account).

        """

        try:
            debtorId, _ = parse_account_uri(account_identity['uri'])
        except ValueError:
            abort(422, errors={'json': {'uri': ['The URI can not be recognized.']}})

        return {'uri': make_debtor_uri(debtorId)}


@accounts_api.route('/<i64:creditorId>/debtor-lookup', parameters=[CID])
class DebtorLookupEndpoint(MethodView):
    @accounts_api.arguments(DebtorIdentitySchema, example=specs.DEBTOR_IDENTITY_EXAMPLE)
    @accounts_api.response(code=204)
    @accounts_api.doc(operationId='debtorLookup',
                      responses={204: specs.NO_ACCOUNT_WITH_THIS_DEBTOR,
                                 303: specs.ACCOUNT_EXISTS})
    def post(self, debtor_identity, creditorId):
        """Try to find an existing account with a given debtor.

        This is useful when the creditor wants not know if he already
        has an account with a given debtor.

        **Note:** A 422 error will be returned when the debtor's
        identity can not be recognized.

        """

        try:
            debtorId = parse_debtor_uri(debtor_identity['uri'])
        except ValueError:
            abort(422, errors={'json': {'uri': ['The URI can not be recognized.']}})

        if procedures.has_account(creditorId, debtorId):
            location = url_for('accounts.AccountEndpoint', _external=True, creditorId=creditorId, debtorId=debtorId)
            return redirect(location, code=303)


@accounts_api.route('/<i64:creditorId>/accounts/', parameters=[CID])
class AccountsEndpoint(MethodView):
    @accounts_api.arguments(AccountsPaginationParamsSchema, location='query')
    @accounts_api.response(ObjectReferencesPageSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountsPage')
    def get(self, params, creditorId):
        """Return a collection of accounts belonging to a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all `Account`s
        belonging to a given creditor. The returned fragment will not
        be sorted in any particular order.

        """

        try:
            prev = u64_to_i64(int(params['prev'])) if 'prev' in params else None
        except ValueError:
            abort(422, errors={'query': {'prev': ['Invalid value.']}})

        n = current_app.config['APP_ACCOUNTS_PER_PAGE']
        debtor_ids = procedures.get_creditor_debtor_ids(creditorId, count=n, prev=prev)
        items = [{'uri': f'{i64_to_u64(debtor_id)}/'} for debtor_id in debtor_ids]

        if len(debtor_ids) < n:
            # The last page does not have a 'next' link.
            return {
                'uri': request.full_path,
                'items': items,
            }

        return {
            'uri': request.full_path,
            'items': items,
            'next': f'?prev={i64_to_u64(debtor_ids[-1])}',
        }

    @accounts_api.arguments(DebtorIdentitySchema, example=specs.DEBTOR_IDENTITY_EXAMPLE)
    @accounts_api.response(AccountSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @accounts_api.doc(operationId='createAccount',
                      responses={303: specs.ACCOUNT_EXISTS,
                                 403: specs.FORBIDDEN_ACCOUNT_OPERATION})
    def post(self, debtor_identity, creditorId):
        """Create account.

        **Note:** This is an idempotent operation.

        """

        try:
            debtorId = parse_debtor_uri(debtor_identity['uri'])
        except ValueError:
            abort(422, errors={'json': {'uri': ['The URI can not be recognized.']}})

        location = url_for('accounts.AccountEndpoint', _external=True, creditorId=creditorId, debtorId=debtorId)
        try:
            inspect_ops.allow_account_creation(creditorId, debtorId)
            account = procedures.create_new_account(creditorId, debtorId)
        except inspect_ops.ForbiddenOperationError:  # pragma: no cover
            abort(403)
        except procedures.CreditorDoesNotExistError:
            abort(404)
        except procedures.AccountExistsError:
            return redirect(location, code=303)

        inspect_ops.register_account_creation(creditorId, debtorId)
        return account, {'Location': location}


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/', parameters=[CID, DID])
class AccountEndpoint(MethodView):
    @accounts_api.response(AccountSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccount')
    def get(self, creditorId, debtorId):
        """Return account.

        The returned `Account` object encompasses all the avilable
        information for a particular account. This includes the
        follwing sub-objects:

        * `AccountConfig`
        * `AccountLedger`
        * `AccountInfo`
        * `AccountDisplay`
        * `AccountExchange`
        * `AccountKnowledge`

        Note that when one of those sub-objects gets changed, a
        `LogEntry` for the change in the particular sub-object will be
        added to the log, but a `LogEntry` for the change in the
        encompassing `Account` object *will not be added to the log*.

        """

        account = procedures.get_account(creditorId, debtorId)
        if account is None:
            abort(404)
        return account

    @accounts_api.response(code=204)
    @accounts_api.doc(operationId='deleteAccount',
                      responses={403: specs.FORBIDDEN_ACCOUNT_DELETION})
    def delete(self, creditorId, debtorId):
        """Delete account.

        This operation will succeed only if all of the following
        conditions are true:

        1. There are no other accounts pegged to this account.

        2. The account is marked as safe for deletion, or unsafe
           deletion is allowed for the account.

        """

        inspect_ops.decrement_account_number(creditorId, debtorId)
        try:
            procedures.delete_account(creditorId, debtorId)
            return
        except procedures.UnsafeAccountDeletionError:
            abort(403)
        except procedures.PegAccountDeletionError:
            abort(403)
        except procedures.AccountDoesNotExistError:
            pass

        # NOTE: We decremented the account number before trying to
        # delete the account, and now when we know that the deletion
        # has been unsuccessful, we increment the account number
        # again. This guarantees that in case of a crash, the
        # difference between the recorded number of accounts and the
        # real number of accounts will always be in users' favor.
        inspect_ops.increment_account_number(creditorId, debtorId)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/config', parameters=[CID, DID])
class AccountConfigEndpoint(MethodView):
    @accounts_api.response(AccountConfigSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountConfig')
    def get(self, creditorId, debtorId):
        """Return account's configuration."""

        config = procedures.get_account_config(creditorId, debtorId)
        if config is None:
            abort(404)
        return config

    @accounts_api.arguments(AccountConfigSchema)
    @accounts_api.response(AccountConfigSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountConfig',
                      responses={403: specs.FORBIDDEN_ACCOUNT_OPERATION,
                                 409: specs.UPDATE_CONFLICT})
    def patch(self, account_config, creditorId, debtorId):
        """Update account's configuration.

        **Note:** This is an idempotent operation.

        """

        try:
            inspect_ops.allow_account_reconfig(creditorId, debtorId)
            config = procedures.update_account_config(
                creditor_id=creditorId,
                debtor_id=debtorId,
                is_scheduled_for_deletion=account_config['is_scheduled_for_deletion'],
                negligible_amount=account_config['negligible_amount'],
                allow_unsafe_deletion=account_config['allow_unsafe_deletion'],
                latest_update_id=account_config['latest_update_id'],
            )
        except inspect_ops.ForbiddenOperationError:  # pragma: no cover
            abort(403)
        except procedures.AccountDoesNotExistError:
            abort(404)
        except procedures.UpdateConflictError:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        inspect_ops.register_account_reconfig(creditorId, debtorId)
        return config


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/display', parameters=[CID, DID])
class AccountDisplayEndpoint(MethodView):
    @accounts_api.response(AccountDisplaySchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountDisplay')
    def get(self, creditorId, debtorId):
        """Return account's display settings."""

        display = procedures.get_account_display(creditorId, debtorId)
        if display is None:
            abort(404)
        return display

    @accounts_api.arguments(AccountDisplaySchema)
    @accounts_api.response(AccountDisplaySchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountDisplay',
                      responses={409: specs.UPDATE_CONFLICT})
    def patch(self, account_display, creditorId, debtorId):
        """Update account's display settings.

        **Note:** This is an idempotent operation.

        """

        optional_debtor_name = account_display.get('optional_debtor_name')
        optional_unit = account_display.get('optional_unit')

        try:
            display = procedures.update_account_display(
                creditor_id=creditorId,
                debtor_id=debtorId,
                debtor_name=optional_debtor_name,
                amount_divisor=account_display['amount_divisor'],
                decimal_places=account_display['decimal_places'],
                unit=optional_unit,
                hide=account_display['hide'],
                latest_update_id=account_display['latest_update_id'],
            )
        except procedures.AccountDoesNotExistError:
            abort(404)
        except procedures.UpdateConflictError:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})
        except procedures.AccountDebtorNameConflictError:
            abort(422, errors={'json': {'debtorName': ['Another account with the same debtorName already exist.']}})

        return display


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/exchange', parameters=[CID, DID])
class AccountExchangeEndpoint(MethodView):
    @accounts_api.response(AccountExchangeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountExchange')
    def get(self, creditorId, debtorId):
        """Return account's exchange settings."""

        exchange = procedures.get_account_exchange(creditorId, debtorId)
        if exchange is None:
            abort(404)
        return exchange

    @accounts_api.arguments(AccountExchangeSchema)
    @accounts_api.response(AccountExchangeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountExchange',
                      responses={409: specs.UPDATE_CONFLICT})
    def patch(self, account_exchange, creditorId, debtorId):
        """Update account's exchange settings.

        **Note:** This is an idempotent operation.

        """

        optional_policy = account_exchange.get('optional_policy')
        optional_peg = account_exchange.get('optional_peg')

        try:
            exchange = procedures.update_account_exchange(
                creditor_id=creditorId,
                debtor_id=debtorId,
                policy=optional_policy,
                min_principal=account_exchange['min_principal'],
                max_principal=account_exchange['max_principal'],
                peg_exchange_rate=optional_peg and optional_peg['exchange_rate'],
                peg_debtor_id=optional_peg and _parse_peg_account_uri(
                    creditor_id=creditorId,
                    base_url=request.full_path,
                    uri=optional_peg['account']['uri'],
                ),
                latest_update_id=account_exchange['latest_update_id'],
            )
        except procedures.AccountDoesNotExistError:
            abort(404)
        except procedures.UpdateConflictError:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})
        except procedures.InvalidExchangePolicyError:
            abort(422, errors={'json': {'policy': ['Invalid policy name.']}})
        except procedures.PegAccountDoesNotExistError:
            abort(422, errors={'json': {'peg': {'account': {'uri': ['Account does not exist.']}}}})

        return exchange


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/knowledge', parameters=[CID, DID])
class AccountKnowledgeEndpoint(MethodView):
    @accounts_api.response(AccountKnowledgeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountKnowledge',
                      responses={409: specs.UPDATE_CONFLICT})
    def get(self, creditorId, debtorId):
        """Return account's stored knowledge.

        The returned object contains previously stored knowledge about
        the account.

        """

        knowledge = procedures.get_account_knowledge(creditorId, debtorId)
        if knowledge is None:
            abort(404)
        return knowledge

    @accounts_api.arguments(AccountKnowledgeSchema)
    @accounts_api.response(AccountKnowledgeSchema(context=CONTEXT))
    @accounts_api.doc(operationId='updateAccountKnowledge',
                      responses={409: specs.UPDATE_CONFLICT})
    def patch(self, account_knowledge, creditorId, debtorId):
        """Update account's stored knowledge.

        This operation should be performed when an important knowledge
        about the account needs to be stored. In addition to the
        properties defined in the `AccountKnowledge` schema, the
        passed object may contain any other properties, which will be
        stored as well. The total length of the stored data can not
        exceed 2000 bytes (JSON, UTF-8 encoded, excluding `type` and
        `latestUpdateId` properties).

        **Note:** This is an idempotent operation.

        """

        try:
            knowledge = procedures.update_account_knowledge(
                creditorId,
                debtorId,
                latest_update_id=account_knowledge['latest_update_id'],
                data=account_knowledge['data'],
            )
        except procedures.AccountDoesNotExistError:
            abort(404)
        except procedures.UpdateConflictError:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        return knowledge


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/info', parameters=[CID, DID])
class AccountInfoEndpoint(MethodView):
    @accounts_api.response(AccountInfoSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountInfo')
    def get(self, creditorId, debtorId):
        """Return account's status information."""

        info = procedures.get_account_info(creditorId, debtorId)
        if info is None:
            abort(404)
        return info


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/ledger', parameters=[CID, DID])
class AccountLedgerEndpoint(MethodView):
    @accounts_api.response(AccountLedgerSchema(context=CONTEXT))
    @accounts_api.doc(operationId='getAccountLedger')
    def get(self, creditorId, debtorId):
        """Return account's ledger."""

        ledger = procedures.get_account_ledger(creditorId, debtorId)
        if ledger is None:
            abort(404)
        return ledger


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[CID, DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(LedgerEntriesPaginationParamsSchema, location='query')
    @accounts_api.response(LedgerEntriesPageSchema(context=CONTEXT), example=specs.ACCOUNT_LEDGER_ENTRIES_EXAMPLE)
    @accounts_api.doc(operationId='getAccountLedgerEntriesPage')
    def get(self, params, creditorId, debtorId):
        """Return a collection of ledger entries for a given account.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains ledger entries for a given
        account. The returned fragment, and all the subsequent
        fragments, will be sorted in reverse-chronological order
        (bigger `entryId`s go first).

        """

        n = current_app.config['APP_LEDGER_ENTRIES_PER_PAGE']
        try:
            ledger_entries = procedures.get_account_ledger_entries(
                creditorId,
                debtorId,
                count=n,
                prev=params['prev'],
                stop=params['stop'],
            )
        except procedures.AccountDoesNotExistError:  # pragma: no cover
            abort(404)

        if len(ledger_entries) < n:
            # The last page does not have a 'next' link.
            return {
                'uri': request.full_path,
                'items': ledger_entries,
            }

        return {
            'uri': request.full_path,
            'items': ledger_entries,
            'next': f'?prev={ledger_entries[-1].entry_id}&stop={params["stop"]}',
        }


transfers_api = Blueprint(
    'transfers',
    __name__,
    url_prefix='/creditors',
    description="Make transfers from one account to another account.",
)


@transfers_api.route('/<i64:creditorId>/transfers/', parameters=[CID])
class TransfersEndpoint(MethodView):
    @transfers_api.arguments(PaginationParametersSchema, location='query')
    @transfers_api.response(ObjectReferencesPageSchema(context=CONTEXT), example=specs.TRANSFER_LINKS_EXAMPLE)
    @transfers_api.doc(operationId='getTransfersPage')
    def get(self, pagination_parameters, creditorId):
        """Return a collection of transfers, initiated by a given creditor.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains references to all transfers
        initiated by a given creditor, which have not been deleted
        yet. The returned fragment will not be sorted in any
        particular order.

        """

        try:
            transfer_uuids = procedures.get_creditor_transfer_uuids(creditorId)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        return transfer_uuids

    @transfers_api.arguments(TransferCreationRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT), code=201, headers=specs.LOCATION_HEADER)
    @transfers_api.doc(operationId='createTransfer',
                       responses={303: specs.TRANSFER_EXISTS,
                                  403: specs.DENIED_TRANSFER,
                                  409: specs.TRANSFER_CONFLICT})
    def post(self, transfer_creation_request, creditorId):
        """Initiate a transfer.

        **Note:** This is an idempotent operation.

        """

        uuid = transfer_creation_request['transfer_uuid']
        location = url_for('transfers.TransferEndpoint', _external=True, creditorId=creditorId, transferUuid=uuid)
        try:
            # TODO: parse `transfer_creation_request['recipient']`.
            debtor_id, recipient = 1, 'xxx'
        except ValueError:
            abort(422, errors={'json': {'recipient': {'uri': ['The URI can not be recognized.']}}})
        try:
            transfer = procedures.initiate_transfer(
                creditor_id=creditorId,
                transfer_uuid=uuid,
                debtor_id=debtor_id,
                recipient=recipient,
                amount=transfer_creation_request['amount'],
                transfer_note=transfer_creation_request['note'],
            )
        except procedures.TooManyManagementActionsError:
            abort(403)
        except procedures.DebtorDoesNotExistError:
            abort(404)
        except procedures.TransfersConflictError:
            abort(409)
        except procedures.TransferExistsError:
            return redirect(location, code=303)
        return transfer, {'Location': location}


@transfers_api.route('/<i64:creditorId>/transfers/<uuid:transferUuid>', parameters=[CID, TRANSFER_UUID])
class TransferEndpoint(MethodView):
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='getTransfer')
    def get(self, creditorId, transferUuid):
        """Return a transfer."""

        return procedures.get_direct_transfer(creditorId, transferUuid) or abort(404)

    @transfers_api.arguments(TransferCancelationRequestSchema)
    @transfers_api.response(TransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='cancelTransfer',
                       responses={403: specs.TRANSFER_CANCELLATION_FAILURE})
    def post(self, cancel_transfer_request, creditorId, transferUuid):
        """Try to cancel a transfer.

        **Note:** This is an idempotent operation.

        """

        try:
            transfer = procedures.cancel_transfer(creditorId, transferUuid)
        except procedures.TransferCancellationError:
            abort(403)
        except procedures.TransferDoesNotExistError:
            abort(404)
        return transfer

    @transfers_api.response(code=204)
    @transfers_api.doc(operationId='deleteTransfer')
    def delete(self, creditorId, transferUuid):
        """Delete a transfer.

        Note that deleting a running (not finalized) transfer does not
        cancel it. To ensure that a running transfer has not been
        successful, it must be canceled before deletion.

        """

        procedures.delete_direct_transfer(creditorId, transferUuid)


@transfers_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/transfers/<transferId>', parameters=[CID, DID, TID])
class CommittedTransferEndpoint(MethodView):
    @transfers_api.response(CommittedTransferSchema(context=CONTEXT))
    @transfers_api.doc(operationId='getCommittedTransfer')
    def get(self, creditorId, debtorId, transferId):
        """Return information about sent or received transfer."""

        try:
            creation_date, transfer_number = _parse_transfer_slug(transferId)
        except ValueError:
            abort(404)

        abort(500)
