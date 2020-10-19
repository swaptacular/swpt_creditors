__version__ = '0.1.0'

import os
import os.path
import logging
import logging.config
from flask_env import MetaFlaskEnv

# Configure app logging. If the value of "$APP_LOGGING_CONFIG_FILE" is
# a relative path, the directory of this (__init__.py) file will be
# used as a current directory.
config_filename = os.environ.get('APP_LOGGING_CONFIG_FILE')
if config_filename:  # pragma: no cover
    if not os.path.isabs(config_filename):
        current_dir = os.path.dirname(__file__)
        config_filename = os.path.join(current_dir, config_filename)
    logging.config.fileConfig(config_filename, disable_existing_loggers=False)
else:
    logging.basicConfig(level=logging.WARNING)


API_DESCRIPTION = """This API can be used to:
1. Get information about creditors, create new creditors.
2. Create, view, update, and delete accounts, view account's transaction history.
3. Make transfers from one account to another account.

The API allows for efficient client-side caching, as well as efficient
cache and data synchronization between two or more clients.

"""


class Configuration(metaclass=MetaFlaskEnv):
    SECRET_KEY = 'dummy-secret'
    SERVER_NAME = None
    PREFERRED_URL_SCHEME = 'http'
    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_POOL_SIZE = None
    SQLALCHEMY_POOL_TIMEOUT = None
    SQLALCHEMY_POOL_RECYCLE = None
    SQLALCHEMY_MAX_OVERFLOW = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    DRAMATIQ_BROKER_CLASS = 'RabbitmqBroker'
    DRAMATIQ_BROKER_URL = 'amqp://guest:guest@localhost:5672'
    API_TITLE = 'Creditors API'
    API_VERSION = 'v1'
    OPENAPI_VERSION = '3.0.2'
    OPENAPI_URL_PREFIX = '/docs'
    OPENAPI_REDOC_PATH = 'redoc'
    OPENAPI_REDOC_URL = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'
    OPENAPI_SWAGGER_UI_PATH = 'swagger-ui'
    OPENAPI_SWAGGER_UI_URL = 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'
    APP_PROCESS_LOG_ENTRIES_THREADS = 1
    APP_PROCESS_LEDGER_UPDATES_THREADS = 1
    APP_PROCESS_LEDGER_UPDATES_BURST = 1000
    APP_CREDITORS_SCAN_DAYS = 7
    APP_CREDITORS_SCAN_BLOCKS_PER_QUERY = 40
    APP_CREDITORS_SCAN_BEAT_MILLISECS = 25
    APP_ACCOUNTS_SCAN_HOURS = 8
    APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY = 160
    APP_ACCOUNTS_SCAN_BEAT_MILLISECS = 100
    APP_LOG_ENTRIES_SCAN_DAYS = 7
    APP_LOG_ENTRIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_LOG_ENTRIES_SCAN_BEAT_MILLISECS = 25
    APP_LEDGER_ENTRIES_SCAN_DAYS = 7
    APP_LEDGER_ENTRIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_LEDGER_ENTRIES_SCAN_BEAT_MILLISECS = 25
    APP_COMMITTED_TRANSFERS_SCAN_DAYS = 7
    APP_COMMITTED_TRANSFERS_SCAN_BLOCKS_PER_QUERY = 100
    APP_COMMITTED_TRANSFERS_SCAN_BEAT_MILLISECS = 35
    APP_TRANSFERS_FINALIZATION_AVG_SECONDS = 5.0
    APP_CREDITORS_PER_PAGE = 2000
    APP_LOG_ENTRIES_PER_PAGE = 100
    APP_ACCOUNTS_PER_PAGE = 100
    APP_TRANSFERS_PER_PAGE = 100
    APP_LEDGER_ENTRIES_PER_PAGE = 100
    APP_LOG_RETENTION_DAYS = 90
    APP_LEDGER_RETENTION_DAYS = 90
    APP_INACTIVE_CREDITOR_RETENTION_DAYS = 14
    APP_DEACTIVATED_CREDITOR_RETENTION_DAYS = 1826
    APP_MAX_HEARTBEAT_DELAY_DAYS = 365
    APP_MAX_TRANSFER_DELAY_DAYS = 14
    APP_MAX_CONFIG_DELAY_HOURS = 24
    APP_PIN_FAILURES_RESET_DAYS = 7
    APP_SUPERUSER_SUBJECT_REGEX = '^creditors:superuser$'
    APP_SUPERVISOR_SUBJECT_REGEX = '^creditors:supervisor$'
    APP_CREDITOR_SUBJECT_REGEX = '^creditors:([0-9]+)$'
    APP_OAUTH2_AUTHORIZATION_URL = '/oauth2/auth'
    APP_OAUTH2_TOKEN_URL = '/oauth2/token'
    APP_OAUTH2_REFRESH_URL = '/oauth2/token'


def generate_api_spec_options(authorizationUrl, tokenUrl, refreshUrl):
    return {
        'info': {
            'description': API_DESCRIPTION,
        },
        'consumes': ['application/json'],
        'produces': ['application/json'],
        'components': {
            'securitySchemes': {
                'oauth2': {
                    'type': 'oauth2',
                    'description': 'This API uses OAuth 2. [More info](https://oauth.net/2/).',
                    'flows': {
                        'authorizationCode': {
                            'authorizationUrl': authorizationUrl,
                            'tokenUrl': tokenUrl,
                            'refreshUrl': refreshUrl,
                            'scopes': {
                                'access': 'read data',
                                'access.modify': 'access and modify data',
                                'disable_pin': 'disable PIN',
                            },
                        },
                        'clientCredentials': {
                            'tokenUrl': tokenUrl,
                            'refreshUrl': refreshUrl,
                            'scopes': {
                                'access': 'read data',
                                'access.modify': 'access and modify data',
                                'disable_pin': 'disable PIN',
                                'activate': 'activate new creditors',
                                'deactivate': 'deactivate existing creditors',
                            },
                        },
                    },
                },
            },
        },
    }


def create_app(config_dict={}):
    from flask import Flask
    from swpt_lib.utils import Int64Converter
    from .extensions import db, migrate, broker, api
    from .routes import admin_api, creditors_api, accounts_api, transfers_api, path_builder
    from .schemas import type_registry
    from .cli import swpt_creditors
    from . import procedures
    from . import models  # noqa

    app = Flask(__name__)
    app.url_map.converters['i64'] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    app.config['API_SPEC_OPTIONS'] = generate_api_spec_options(
        authorizationUrl=app.config['APP_OAUTH2_AUTHORIZATION_URL'],
        tokenUrl=app.config['APP_OAUTH2_TOKEN_URL'],
        refreshUrl=app.config['APP_OAUTH2_REFRESH_URL'],
    )
    db.init_app(app)
    migrate.init_app(app, db)
    broker.init_app(app)
    api.init_app(app)
    api.register_blueprint(admin_api)
    api.register_blueprint(creditors_api)
    api.register_blueprint(accounts_api)
    api.register_blueprint(transfers_api)
    app.cli.add_command(swpt_creditors)
    procedures.init(path_builder, type_registry)
    return app
