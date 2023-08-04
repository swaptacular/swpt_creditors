import re
from typing import Tuple, Optional
from enum import IntEnum
from datetime import date, timedelta, datetime, timezone
from flask import url_for, current_app, request, g
from flask_smorest import abort, Blueprint as BlueprintOrig
from swpt_pythonlib.utils import u64_to_i64
from swpt_creditors.models import MAX_INT64, DATE0, PinInfo
from swpt_creditors.schemas import type_registry

NOT_REQUIED = 'false'
READ_ONLY_METHODS = ['GET', 'HEAD', 'OPTIONS']


class Blueprint(BlueprintOrig):
    """A Blueprint subclass to use, that we may want to modify."""


class UserType(IntEnum):
    SUPERUSER = 1
    SUPERVISOR = 2
    CREDITOR = 3


class UserIdPatternMatcher:
    PATTERN_CONFIG_KEYS = {
        UserType.SUPERUSER: 'APP_SUPERUSER_SUBJECT_REGEX',
        UserType.SUPERVISOR: 'APP_SUPERVISOR_SUBJECT_REGEX',
        UserType.CREDITOR: 'APP_CREDITOR_SUBJECT_REGEX',
    }

    def __init__(self):
        self._regex_patterns = {}

    def get_pattern(self, user_type: UserType) -> re.Pattern:
        pattern_config_key = self.PATTERN_CONFIG_KEYS[user_type]
        regex = current_app.config[pattern_config_key]
        regex_patterns = self._regex_patterns
        regex_pattern = regex_patterns.get(regex)
        if regex_pattern is None:
            regex_pattern = regex_patterns[regex] = re.compile(regex)

        return regex_pattern

    def match(self, user_id: str) -> Tuple[UserType, Optional[int]]:
        for user_type in UserType:
            pattern = self.get_pattern(user_type)
            m = pattern.match(user_id)
            if m:
                creditor_id = u64_to_i64(int(m.group(1))) if user_type == UserType.CREDITOR else None
                return user_type, creditor_id

        abort(403)


user_id_pattern_matcher = UserIdPatternMatcher()


def parse_swpt_user_id_header() -> Tuple[UserType, Optional[int]]:
    user_id = request.headers.get('X-Swpt-User-Id')
    if user_id is None:
        user_type = UserType.SUPERUSER
        creditor_id = None
    else:
        user_type, creditor_id = user_id_pattern_matcher.match(user_id)

    g.superuser = user_type == UserType.SUPERUSER
    return user_type, creditor_id


def ensure_admin():
    user_type, _ = parse_swpt_user_id_header()
    if user_type == UserType.CREDITOR:
        abort(403)


def ensure_creditor_permissions():
    # NOTE: Creditors can access and modify only their own resources.
    # Supervisors can activate new creditors, and have read-only
    # access to all creditor's resources. Superusers are allowed
    # everything.

    user_type, creditor_id = parse_swpt_user_id_header()
    url_creditor_id = request.view_args.get('creditorId')
    if url_creditor_id is None:
        url_creditor_id = creditor_id
    else:
        assert isinstance(url_creditor_id, int)
        if not current_app.config['SHARDING_REALM'].match(url_creditor_id):
            abort(500)  # pragma: no cover

    if user_type == UserType.CREDITOR and creditor_id != url_creditor_id:
        abort(403)

    if user_type == UserType.SUPERVISOR and request.method not in READ_ONLY_METHODS:
        abort(403)

    x_swpt_require_pin = request.headers.get('X-Swpt-Require-Pin', NOT_REQUIED)
    g.pin_reset_mode = x_swpt_require_pin == NOT_REQUIED
    g.creditor_id = creditor_id


def make_transfer_slug(creation_date: date, transfer_number: int) -> str:
    epoch = (creation_date - DATE0).days
    return f'{epoch}-{transfer_number}'


def parse_transfer_slug(slug) -> Tuple[date, int]:
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


def calc_checkup_datetime(debtor_id: int, initiated_at: datetime) -> datetime:
    current_ts = datetime.now(tz=timezone.utc)
    current_delay = current_ts - initiated_at
    average_delay = timedelta(seconds=current_app.config['APP_TRANSFERS_FINALIZATION_APPROX_SECONDS'])
    return current_ts + max(current_delay, average_delay)


def calc_log_retention_days(creditor_id: int) -> int:
    return int(current_app.config['APP_LOG_RETENTION_DAYS'])


def calc_reservation_deadline(created_at: datetime) -> datetime:
    return created_at + timedelta(days=current_app.config['APP_INACTIVE_CREDITOR_RETENTION_DAYS'])


def calc_require_pin(pin_info: PinInfo) -> bool:
    return not g.pin_reset_mode and pin_info.is_required


class path_builder:
    def _build_committed_transfer_path(creditorId, debtorId, creationDate, transferNumber):
        with current_app.test_request_context():
            return url_for(
                'transfers.CommittedTransferEndpoint',
                creditorId=creditorId,
                debtorId=debtorId,
                transferId=make_transfer_slug(creationDate, transferNumber),
                _external=False,
            )

    def _url_for(name):
        @staticmethod
        def m(**kw):
            with current_app.test_request_context():
                return url_for(name, _external=False, **kw)

        return m

    creditors_list = _url_for('admin.CreditorsListEndpoint')
    creditor_enumerate = _url_for('admin.CreditorEnumerateEndpoint')
    creditor = _url_for('creditors.CreditorEndpoint')
    pin_info = _url_for('creditors.PinInfoEndpoint')
    wallet = _url_for('creditors.WalletEndpoint')
    log_entries = _url_for('creditors.LogEntriesEndpoint')
    debtor_lookup = _url_for('accounts.DebtorLookupEndpoint')
    account_lookup = _url_for('accounts.AccountLookupEndpoint')
    account = _url_for('accounts.AccountEndpoint')
    account_info = _url_for('accounts.AccountInfoEndpoint')
    account_config = _url_for('accounts.AccountConfigEndpoint')
    account_display = _url_for('accounts.AccountDisplayEndpoint')
    account_exchange = _url_for('accounts.AccountExchangeEndpoint')
    account_knowledge = _url_for('accounts.AccountKnowledgeEndpoint')
    account_ledger = _url_for('accounts.AccountLedgerEndpoint')
    account_ledger_entries = _url_for('accounts.AccountLedgerEntriesEndpoint')
    accounts_list = _url_for('creditors.AccountsListEndpoint')
    accounts = _url_for('accounts.AccountsEndpoint')
    transfer = _url_for('transfers.TransferEndpoint')
    transfers_list = _url_for('creditors.TransfersListEndpoint')
    transfers = _url_for('transfers.TransfersEndpoint')
    committed_transfer = _build_committed_transfer_path


context = {
    'paths': path_builder,
    'types': type_registry,
    'calc_checkup_datetime': calc_checkup_datetime,
    'calc_log_retention_days': calc_log_retention_days,
    'calc_reservation_deadline': calc_reservation_deadline,
    'calc_require_pin': calc_require_pin,
}
