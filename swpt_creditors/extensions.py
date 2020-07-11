import os
import warnings
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_signalbus import SignalBusMixin, AtomicProceduresMixin
from flask_melodramatiq import RabbitmqBroker
from dramatiq import Middleware
from flask_smorest import Api

MAIN_EXCHANGE_NAME = 'dramatiq'
APP_QUEUE_NAME = os.environ.get('APP_QUEUE_NAME', 'swpt_creditors')

warnings.filterwarnings(
    'ignore',
    r"Reset agent is not active.  This should not occur unless there was already a connectivity error in progress",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


class EventSubscriptionMiddleware(Middleware):
    @property
    def actor_options(self):
        return {'event_subscription'}


db = CustomAlchemy()
migrate = Migrate()
broker = RabbitmqBroker(confirm_delivery=True)
broker.add_middleware(EventSubscriptionMiddleware())
api = Api()
