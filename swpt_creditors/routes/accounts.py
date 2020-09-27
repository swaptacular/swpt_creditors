from urllib.parse import urlsplit, urljoin
from werkzeug.routing import NotFound, RequestRedirect, MethodNotAllowed
from flask import current_app, redirect, url_for, request
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from swpt_lib.utils import i64_to_u64, u64_to_i64
from swpt_lib.swpt_uris import parse_debtor_uri, parse_account_uri, make_debtor_uri
from swpt_creditors.schemas import examples, DebtorIdentitySchema, AccountIdentitySchema, \
    AccountSchema, AccountConfigSchema, AccountDisplaySchema, AccountExchangeSchema, \
    AccountKnowledgeSchema, AccountInfoSchema, AccountLedgerSchema, ObjectReferencesPageSchema, \
    AccountsPaginationParamsSchema, LedgerEntriesPaginationParamsSchema, LedgerEntriesPageSchema
from swpt_creditors import procedures
from swpt_creditors import inspect_ops
from .common import context, verify_creditor_id
from .specs import DID, CID
from . import specs


def _parse_peg_account_uri(creditor_id: int, base_url: str, uri: str) -> int:
    Error = procedures.PegDoesNotExist

    try:
        scheme, netloc, path, *rest = urlsplit(urljoin(base_url, uri))
    except ValueError:
        raise Error()

    base_url_scheme, base_url_netloc, *_ = urlsplit(request.base_url)
    if any(rest) or (scheme and scheme != base_url_scheme) or (netloc and netloc != base_url_netloc):
        raise Error()

    try:
        endpoint, params = current_app.url_map.bind('localhost').match(path)
    except (NotFound, RequestRedirect, MethodNotAllowed):
        raise Error()

    if endpoint != 'accounts.AccountEndpoint' or params['creditorId'] != creditor_id:
        raise Error()

    return params['debtorId']


accounts_api = Blueprint(
    'accounts',
    __name__,
    url_prefix='/creditors',
    description="Create, view, update, and delete accounts, view account's transaction history.",
)
accounts_api.before_request(verify_creditor_id)


@accounts_api.route('/<i64:creditorId>/account-lookup', parameters=[CID])
class AccountLookupEndpoint(MethodView):
    @accounts_api.arguments(AccountIdentitySchema, example=examples.ACCOUNT_IDENTITY_EXAMPLE)
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
    @accounts_api.arguments(DebtorIdentitySchema, example=examples.DEBTOR_IDENTITY_EXAMPLE)
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
    @accounts_api.response(ObjectReferencesPageSchema(context=context))
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

        n = int(current_app.config['APP_ACCOUNTS_PER_PAGE'])
        debtor_ids = procedures.get_account_debtor_ids(creditorId, count=n, prev=prev)
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

    @accounts_api.arguments(DebtorIdentitySchema, example=examples.DEBTOR_IDENTITY_EXAMPLE)
    @accounts_api.response(AccountSchema(context=context), code=201, headers=specs.LOCATION_HEADER)
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
        except inspect_ops.ForbiddenOperation:  # pragma: no cover
            abort(403)
        except procedures.CreditorDoesNotExist:
            abort(404)
        except procedures.AccountExists:
            return redirect(location, code=303)

        inspect_ops.register_account_creation(creditorId, debtorId)
        return account, {'Location': location}


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/', parameters=[CID, DID])
class AccountEndpoint(MethodView):
    @accounts_api.response(AccountSchema(context=context))
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

        return procedures.get_account(creditorId, debtorId) or abort(404)

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

        deleted = False
        inspect_ops.decrement_account_number(creditorId, debtorId)
        try:
            procedures.delete_account(creditorId, debtorId)
            deleted = True
        except procedures.UnsafeAccountDeletion:
            abort(403)
        except procedures.ForbiddenPegDeletion:
            abort(403)
        except (procedures.CreditorDoesNotExist, procedures.AccountDoesNotExist):
            return
        finally:
            # NOTE: We decremented the account number before trying to
            # delete the account. Now if the deletion has been
            # unsuccessful, we should increment the account number
            # again. This guarantees that in case of a crash, the
            # difference between the recorded number of accounts and
            # the real number of accounts will always be in users'
            # favor.
            deleted or inspect_ops.increment_account_number(creditorId, debtorId)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/config', parameters=[CID, DID])
class AccountConfigEndpoint(MethodView):
    @accounts_api.response(AccountConfigSchema(context=context))
    @accounts_api.doc(operationId='getAccountConfig')
    def get(self, creditorId, debtorId):
        """Return account's configuration."""

        return procedures.get_account_config(creditorId, debtorId) or abort(404)

    @accounts_api.arguments(AccountConfigSchema)
    @accounts_api.response(AccountConfigSchema(context=context))
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
        except inspect_ops.ForbiddenOperation:  # pragma: no cover
            abort(403)
        except procedures.AccountDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        inspect_ops.register_account_reconfig(creditorId, debtorId)
        return config


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/display', parameters=[CID, DID])
class AccountDisplayEndpoint(MethodView):
    @accounts_api.response(AccountDisplaySchema(context=context))
    @accounts_api.doc(operationId='getAccountDisplay')
    def get(self, creditorId, debtorId):
        """Return account's display settings."""

        return procedures.get_account_display(creditorId, debtorId) or abort(404)

    @accounts_api.arguments(AccountDisplaySchema)
    @accounts_api.response(AccountDisplaySchema(context=context))
    @accounts_api.doc(operationId='updateAccountDisplay',
                      responses={409: specs.UPDATE_CONFLICT})
    def patch(self, account_display, creditorId, debtorId):
        """Update account's display settings.

        **Note:** This is an idempotent operation.

        """

        try:
            display = procedures.update_account_display(
                creditor_id=creditorId,
                debtor_id=debtorId,
                debtor_name=account_display.get('optional_debtor_name'),
                amount_divisor=account_display['amount_divisor'],
                decimal_places=account_display['decimal_places'],
                unit=account_display.get('optional_unit'),
                hide=account_display['hide'],
                latest_update_id=account_display['latest_update_id'],
            )
        except procedures.AccountDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})
        except procedures.DebtorNameConflict:
            abort(422, errors={'json': {'debtorName': ['Another account with the same debtorName already exist.']}})

        return display


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/exchange', parameters=[CID, DID])
class AccountExchangeEndpoint(MethodView):
    @accounts_api.response(AccountExchangeSchema(context=context))
    @accounts_api.doc(operationId='getAccountExchange')
    def get(self, creditorId, debtorId):
        """Return account's exchange settings."""

        return procedures.get_account_exchange(creditorId, debtorId) or abort(404)

    @accounts_api.arguments(AccountExchangeSchema)
    @accounts_api.response(AccountExchangeSchema(context=context))
    @accounts_api.doc(operationId='updateAccountExchange',
                      responses={409: specs.UPDATE_CONFLICT})
    def patch(self, account_exchange, creditorId, debtorId):
        """Update account's exchange settings.

        **Note:** This is an idempotent operation.

        """

        optional_peg = account_exchange.get('optional_peg')
        try:
            exchange = procedures.update_account_exchange(
                creditor_id=creditorId,
                debtor_id=debtorId,
                policy=account_exchange.get('optional_policy'),
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
        except procedures.AccountDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})
        except procedures.InvalidPolicyName:
            abort(422, errors={'json': {'policy': ['Invalid policy name.']}})
        except procedures.PegDoesNotExist:
            abort(422, errors={'json': {'peg': {'account': {'uri': ['Account does not exist.']}}}})

        return exchange


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/knowledge', parameters=[CID, DID])
class AccountKnowledgeEndpoint(MethodView):
    @accounts_api.response(AccountKnowledgeSchema(context=context))
    @accounts_api.doc(operationId='getAccountKnowledge',
                      responses={409: specs.UPDATE_CONFLICT})
    def get(self, creditorId, debtorId):
        """Return account's stored knowledge.

        The returned object contains previously stored knowledge about
        the account.

        """

        return procedures.get_account_knowledge(creditorId, debtorId) or abort(404)

    @accounts_api.arguments(AccountKnowledgeSchema)
    @accounts_api.response(AccountKnowledgeSchema(context=context))
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
        except procedures.AccountDoesNotExist:
            abort(404)
        except procedures.UpdateConflict:
            abort(409, errors={'json': {'latestUpdateId': ['Incorrect value.']}})

        return knowledge


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/info', parameters=[CID, DID])
class AccountInfoEndpoint(MethodView):
    @accounts_api.response(AccountInfoSchema(context=context))
    @accounts_api.doc(operationId='getAccountInfo')
    def get(self, creditorId, debtorId):
        """Return account's status information."""

        return procedures.get_account_info(creditorId, debtorId) or abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/ledger', parameters=[CID, DID])
class AccountLedgerEndpoint(MethodView):
    @accounts_api.response(AccountLedgerSchema(context=context))
    @accounts_api.doc(operationId='getAccountLedger')
    def get(self, creditorId, debtorId):
        """Return account's ledger."""

        return procedures.get_account_ledger(creditorId, debtorId) or abort(404)


@accounts_api.route('/<i64:creditorId>/accounts/<i64:debtorId>/entries', parameters=[CID, DID])
class AccountLedgerEntriesEndpoint(MethodView):
    @accounts_api.arguments(LedgerEntriesPaginationParamsSchema, location='query')
    @accounts_api.response(LedgerEntriesPageSchema(context=context), example=examples.ACCOUNT_LEDGER_ENTRIES_EXAMPLE)
    @accounts_api.doc(operationId='getAccountLedgerEntriesPage')
    def get(self, params, creditorId, debtorId):
        """Return a collection of ledger entries for a given account.

        The returned object will be a fragment (a page) of a paginated
        list. The paginated list contains ledger entries for a given
        account. The returned fragment, and all the subsequent
        fragments, will be sorted in reverse-chronological order
        (bigger `entryId`s go first).

        """

        n = int(current_app.config['APP_LEDGER_ENTRIES_PER_PAGE'])
        prev = params['prev']
        stop = params['stop']
        try:
            ledger_entries = procedures.get_account_ledger_entries(creditorId, debtorId, count=n, prev=prev, stop=stop)
        except procedures.AccountDoesNotExist:  # pragma: no cover
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
            'next': f'?prev={ledger_entries[-1].entry_id}&stop={stop}',
        }
