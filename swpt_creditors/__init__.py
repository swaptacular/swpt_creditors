__version__ = '0.1.0'

import os
import os.path
import logging
import logging.config
from flask_env import MetaFlaskEnv
from swpt_lib import endpoints

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
    API_SPEC_OPTIONS = {
        'info': {
            'description': API_DESCRIPTION,
        },
        'consumes': ['application/json'],
        'produces': ['application/json'],
    }
    OPENAPI_VERSION = '3.0.2'
    OPENAPI_URL_PREFIX = '/docs'
    OPENAPI_REDOC_PATH = 'redoc'
    OPENAPI_REDOC_URL = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'
    OPENAPI_SWAGGER_UI_PATH = 'swagger-ui'
    OPENAPI_SWAGGER_UI_URL = 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'
    APP_TRANSFERS_FINALIZATION_AVG_SECONDS = 5.0
    APP_DEAD_ACCOUNTS_ABANDON_DAYS = 365
    APP_LOG_ENTRIES_PER_PAGE = 100


def create_app(config_dict={}):
    from flask import Flask
    from swpt_lib.utils import Int64Converter
    from .extensions import db, migrate, broker, api
    from .routes import creditors_api, accounts_api, transfers_api
    from .cli import swpt_creditors
    from . import models  # noqa

    app = Flask(__name__)
    app.url_map.converters['i64'] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(dict(
        SERVER_NAME=endpoints.get_server_name(),
        PREFERRED_URL_SCHEME=endpoints.get_url_scheme(),
    ))
    app.config.from_mapping(config_dict)
    db.init_app(app)
    migrate.init_app(app, db)
    broker.init_app(app)
    api.init_app(app)
    api.register_blueprint(creditors_api)
    api.register_blueprint(accounts_api)
    api.register_blueprint(transfers_api)
    app.cli.add_command(swpt_creditors)
    return app
