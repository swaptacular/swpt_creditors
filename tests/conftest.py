import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone
from swpt_creditors import create_app
from swpt_creditors.extensions import db

config_dict = {
    "TESTING": True,
    "PREFERRED_URL_SCHEME": "http",
    "MIN_CREDITOR_ID": 4294967296,
    "MAX_CREDITOR_ID": 8589934591,
    "APP_ENABLE_CORS": True,
    "APP_TRANSFERS_FINALIZATION_APPROX_SECONDS": 10.0,
    "APP_PROCESS_LEDGER_UPDATES_BURST": 1,
    "APP_MAX_TRANSFERS_PER_MONTH": 10,
    "APP_CREDITORS_PER_PAGE": 2,
    "APP_LOG_ENTRIES_PER_PAGE": 2,
    "APP_ACCOUNTS_PER_PAGE": 2,
    "APP_TRANSFERS_PER_PAGE": 2,
    "APP_LEDGER_ENTRIES_PER_PAGE": 2,
    "APP_LOG_RETENTION_DAYS": 31.0,
    "APP_LEDGER_RETENTION_DAYS": 31.0,
    "APP_MAX_TRANSFER_DELAY_DAYS": 14.0,
    "APP_INACTIVE_CREDITOR_RETENTION_DAYS": 14.0,
    "APP_DEACTIVATED_CREDITOR_RETENTION_DAYS": 1826.0,
    "APP_PIN_FAILURES_RESET_DAYS": 7.0,
    "APP_MAX_CREDITOR_ACCOUNTS": 32000,
    "APP_MAX_CREDITOR_TRANSFERS": 32000,
    "APP_MAX_CREDITOR_RECONFIGS": 32000,
    "APP_MAX_CREDITOR_INITIATIONS": 32000,
    "APP_CREDITOR_DOS_STATS_CLEAR_HOURS": 168.0,
    "APP_VERIFY_SHARD_YIELD_PER": 1,
}


def pytest_addoption(parser):
    parser.addoption("--use-pgplsql", action="store", default="false")


@pytest.fixture(scope="module")
def app(request):
    """Get a Flask application object."""

    config_dict["APP_USE_PGPLSQL_FUNCTIONS"] = (
        request.config.option.use_pgplsql.lower() not in ["false", "no", "off"]
    )
    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope="function")
def db_session(app):
    """Get a Flask-SQLAlchmey session, with an automatic cleanup."""

    yield db.session

    # Cleanup:
    db.session.remove()
    for cmd in [
        "TRUNCATE TABLE creditor CASCADE",
        "TRUNCATE TABLE ledger_entry",
        "TRUNCATE TABLE log_entry",
        "TRUNCATE TABLE pending_log_entry",
        "TRUNCATE TABLE committed_transfer",
        "TRUNCATE TABLE configure_account_signal",
        "TRUNCATE TABLE prepare_transfer_signal",
        "TRUNCATE TABLE finalize_transfer_signal",
        "TRUNCATE TABLE updated_ledger_signal",
        "TRUNCATE TABLE updated_policy_signal",
        "TRUNCATE TABLE updated_flags_signal",
        "TRUNCATE TABLE rejected_config_signal",
        "TRUNCATE TABLE usage_stats",
    ]:
        db.session.execute(sqlalchemy.text(cmd))
    db.session.commit()


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)
