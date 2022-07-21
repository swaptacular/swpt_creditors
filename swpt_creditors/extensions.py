import os
import warnings
from json import dumps
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_signalbus import SignalBusMixin, AtomicProceduresMixin, rabbitmq
from flask_melodramatiq import RabbitmqBroker
from dramatiq import Middleware
from flask_smorest import Api

MAIN_EXCHANGE_NAME = 'dramatiq'
APP_QUEUE_NAME = os.environ.get('APP_QUEUE_NAME', 'swpt_creditors')
CREDITORS_OUT_EXCHANGE = 'creditors_out'
CREDITORS_IN_EXCHANGE = 'creditors_in'

warnings.filterwarnings(
    'ignore',
    r"Reset agent is not active.  This should not occur unless there was already a connectivity error in progress",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    def apply_driver_hacks(self, app, info, options):
        separators = (',', ':')
        options["json_serializer"] = lambda obj: dumps(obj, ensure_ascii=False, allow_nan=False, separators=separators)
        return super().apply_driver_hacks(app, info, options)


class EventSubscriptionMiddleware(Middleware):
    @property
    def actor_options(self):
        return {'event_subscription'}


db = CustomAlchemy()
db.signalbus.autoflush = False
migrate = Migrate()
protocol_broker = RabbitmqBroker(config_prefix='PROTOCOL_BROKER', confirm_delivery=True)
protocol_broker.add_middleware(EventSubscriptionMiddleware())
publisher = rabbitmq.Publisher(url_config_key='PROTOCOL_BROKER_URL')
api = Api()
