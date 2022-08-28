__version__ = '0.1.0'

import logging
import json
import sys
import os
import os.path
from typing import List
from flask_cors import CORS


def _parse_dict(s: str) -> dict:
    try:
        return json.loads(s)
    except ValueError:  # pragma: no cover
        raise ValueError(f'Invalid JSON configuration value: {s}')


def _excepthook(exc_type, exc_value, traceback):  # pragma: nocover
    logging.error("Uncaught exception occured", exc_info=(exc_type, exc_value, traceback))


def _remove_handlers(logger):
    for h in logger.handlers:
        logger.removeHandler(h)  # pragma: nocover


def _add_console_hander(logger, format: str):
    handler = logging.StreamHandler(sys.stdout)
    fmt = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'

    if format == 'text':
        handler.setFormatter(logging.Formatter(fmt))
    elif format == 'json':  # pragma: nocover
        from pythonjsonlogger import jsonlogger
        handler.setFormatter(jsonlogger.JsonFormatter(fmt))
    else:  # pragma: nocover
        raise RuntimeError(f'invalid log format: {format}')

    handler.addFilter(_filter_pika_connection_reset_errors)
    logger.addHandler(handler)


def _configure_root_logger(format: str) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    _remove_handlers(root_logger)
    _add_console_hander(root_logger, format)

    return root_logger


def _filter_pika_connection_reset_errors(record: logging.LogRecord) -> bool:  # pragma: nocover
    # NOTE: Currently, when one of Pika's connections to the RabbitMQ
    # server has not been used for some time, it will be closed by the
    # server. We successfully recover form these situations, but pika
    # logs a bunch of annoying errors. Here we filter out those
    # errors.

    message = record.getMessage()
    is_pika_connection_reset_error = record.levelno == logging.ERROR and (
        (
            record.name == 'pika.adapters.utils.io_services_utils'
            and message.startswith(
                "_AsyncBaseTransport._produce() failed, aborting connection: "
                "error=ConnectionResetError(104, 'Connection reset by peer'); "
            )
        ) or (
            record.name == 'pika.adapters.base_connection'
            and message.startswith(
                "connection_lost: StreamLostError: "
                "(\"Stream connection lost: ConnectionResetError(104, 'Connection reset by peer')\",)"
            )
        ) or (
            record.name == 'pika.adapters.blocking_connection'
            and message.startswith(
                "Unexpected connection close detected: StreamLostError: "
                "(\"Stream connection lost: ConnectionResetError(104, 'Connection reset by peer')\",)"
            )
        )
    )

    return not is_pika_connection_reset_error


def configure_logging(level: str, format: str, associated_loggers: List[str]) -> None:
    root_logger = _configure_root_logger(format)

    # Set the log level for this app's logger.
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level.upper())
    app_logger_level = app_logger.getEffectiveLevel()

    # Make sure that all loggers that are associated to this app have
    # their log levels set to the specified level as well.
    for qualname in associated_loggers:
        logging.getLogger(qualname).setLevel(app_logger_level)

    # Make sure that the root logger's log level (that is: the log
    # level for all third party libraires) is not lower than the
    # specified level.
    if app_logger_level > root_logger.getEffectiveLevel():
        root_logger.setLevel(app_logger_level)  # pragma: no cover

    # Delete all gunicorn's log handlers (they are not needed in a
    # docker container because everything goes to the stdout anyway),
    # and make sure that the gunicorn logger's log level is not lower
    # than the specified level.
    gunicorn_logger = logging.getLogger('gunicorn.error')
    gunicorn_logger.propagate = True
    _remove_handlers(gunicorn_logger)
    if app_logger_level > gunicorn_logger.getEffectiveLevel():
        gunicorn_logger.setLevel(app_logger_level)  # pragma: no cover


class MetaEnvReader(type):
    def __init__(cls, name, bases, dct):
        """MetaEnvReader class initializer.

        This function will get called when a new class which utilizes
        this metaclass is defined, as opposed to when an instance is
        initialized. This function overrides the default configuration
        from environment variables.

        """

        super().__init__(name, bases, dct)
        NoneType = type(None)
        annotations = dct.get('__annotations__', {})
        falsy_values = {'false', 'off', 'no', ''}
        for key, value in os.environ.items():
            if hasattr(cls, key):
                target_type = annotations.get(key) or type(getattr(cls, key))
                if target_type is NoneType:  # pragma: no cover
                    target_type = str

                if target_type is bool:
                    value = value.lower() not in falsy_values
                else:
                    value = target_type(value)

                setattr(cls, key, value)


class Configuration(metaclass=MetaEnvReader):
    MIN_CREDITOR_ID: int = None
    MAX_CREDITOR_ID: int = None

    PIN_PROTECTION_SECRET = ''

    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_ENGINE_OPTIONS: _parse_dict = _parse_dict('{"pool_size": 0}')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False

    REDIS_URL = 'redis://localhost:6379/0?health_check_interval=30'

    PROTOCOL_BROKER_URL = 'amqp://guest:guest@localhost:5672'
    PROTOCOL_BROKER_QUEUE = 'swpt_creditors'
    PROTOCOL_BROKER_QUEUE_ROUTING_KEY = '#'
    PROTOCOL_BROKER_PROCESSES = 1
    PROTOCOL_BROKER_THREADS = 1
    PROTOCOL_BROKER_PREFETCH_SIZE = 0
    PROTOCOL_BROKER_PREFETCH_COUNT = 1

    PROCESS_LOG_ADDITIONS_THREADS = 1
    PROCESS_LEDGER_UPDATES_THREADS = 1

    API_TITLE = 'Creditors API'
    API_VERSION = 'v1'
    OPENAPI_VERSION = '3.0.2'
    OPENAPI_URL_PREFIX = '/creditors/.docs'
    OPENAPI_REDOC_PATH = ''
    OPENAPI_REDOC_URL = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'
    OPENAPI_SWAGGER_UI_PATH = 'swagger-ui'
    OPENAPI_SWAGGER_UI_URL = None  # or 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'

    APP_ENABLE_CORS = False
    APP_PROCESS_LOG_ADDITIONS_WAIT = 5.0
    APP_PROCESS_LOG_ADDITIONS_MAX_COUNT = 100000
    APP_PROCESS_LEDGER_UPDATES_BURST = 1000
    APP_PROCESS_LEDGER_UPDATES_MAX_COUNT = 100000
    APP_PROCESS_LEDGER_UPDATES_WAIT = 5.0
    APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT = 10000
    APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT = 10000
    APP_CREDITORS_SCAN_DAYS = 7.0
    APP_CREDITORS_SCAN_BLOCKS_PER_QUERY = 40
    APP_CREDITORS_SCAN_BEAT_MILLISECS = 25
    APP_ACCOUNTS_SCAN_HOURS = 8.0
    APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY = 160
    APP_ACCOUNTS_SCAN_BEAT_MILLISECS = 100
    APP_LOG_ENTRIES_SCAN_DAYS = 7.0
    APP_LOG_ENTRIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_LOG_ENTRIES_SCAN_BEAT_MILLISECS = 25
    APP_LEDGER_ENTRIES_SCAN_DAYS = 7.0
    APP_LEDGER_ENTRIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_LEDGER_ENTRIES_SCAN_BEAT_MILLISECS = 25
    APP_COMMITTED_TRANSFERS_SCAN_DAYS = 7.0
    APP_COMMITTED_TRANSFERS_SCAN_BLOCKS_PER_QUERY = 100
    APP_COMMITTED_TRANSFERS_SCAN_BEAT_MILLISECS = 35
    APP_TRANSFERS_FINALIZATION_APPROX_SECONDS = 20.0
    APP_CREDITORS_PER_PAGE = 2000
    APP_LOG_ENTRIES_PER_PAGE = 100
    APP_ACCOUNTS_PER_PAGE = 100
    APP_TRANSFERS_PER_PAGE = 100
    APP_LEDGER_ENTRIES_PER_PAGE = 100
    APP_LOG_RETENTION_DAYS = 90.0
    APP_LEDGER_RETENTION_DAYS = 90.0
    APP_INACTIVE_CREDITOR_RETENTION_DAYS = 14.0
    APP_DEACTIVATED_CREDITOR_RETENTION_DAYS = 1826.0
    APP_MAX_HEARTBEAT_DELAY_DAYS = 365.0
    APP_MAX_TRANSFER_DELAY_DAYS = 14.0
    APP_MAX_CONFIG_DELAY_HOURS = 24.0
    APP_PIN_FAILURES_RESET_DAYS = 7.0
    APP_MAX_CREDITOR_ACCOUNTS = 1000
    APP_MAX_CREDITOR_TRANSFERS = 20000
    APP_MAX_CREDITOR_RECONFIGS = 5000
    APP_MAX_CREDITOR_INITIATIONS = 20000
    APP_CREDITOR_DOS_STATS_CLEAR_HOURS = 168.0
    APP_SUPERUSER_SUBJECT_REGEX = '^creditors-superuser$'
    APP_SUPERVISOR_SUBJECT_REGEX = '^creditors-supervisor$'
    APP_CREDITOR_SUBJECT_REGEX = '^creditors:([0-9]+)$'


assert Configuration.APP_LOG_RETENTION_DAYS >= 30, 'APP_LOG_RETENTION_DAYS must be at least 30.'


def create_app(config_dict={}):
    from werkzeug.middleware.proxy_fix import ProxyFix
    from flask import Flask
    from swpt_pythonlib.utils import Int64Converter
    from .extensions import db, migrate, api, publisher, redis_store
    from .routes import admin_api, creditors_api, accounts_api, transfers_api, path_builder, specs
    from .schemas import type_registry
    from .cli import swpt_creditors
    from . import procedures
    from . import models  # noqa

    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_port=1)
    app.url_map.converters['i64'] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    app.config['API_SPEC_OPTIONS'] = specs.API_SPEC_OPTIONS
    if app.config['APP_ENABLE_CORS']:
        CORS(app, max_age=24 * 60 * 60, vary_header=False, expose_headers=['Location'])
    db.init_app(app)
    migrate.init_app(app, db)
    redis_store.init_app(app)
    publisher.init_app(app)
    api.init_app(app)
    api.register_blueprint(admin_api)
    api.register_blueprint(creditors_api)
    api.register_blueprint(accounts_api)
    api.register_blueprint(transfers_api)
    app.cli.add_command(swpt_creditors)
    procedures.init(path_builder, type_registry)
    return app


configure_logging(
    level=os.environ.get('APP_LOG_LEVEL', 'warning'),
    format=os.environ.get('APP_LOG_FORMAT', 'text'),
    associated_loggers=os.environ.get('APP_ASSOCIATED_LOGGERS', '').split(),
)
sys.excepthook = _excepthook
