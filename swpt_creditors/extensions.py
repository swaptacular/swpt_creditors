import warnings
from json import dumps
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from swpt_pythonlib.flask_signalbus import SignalBusMixin, AtomicProceduresMixin
from swpt_pythonlib import rabbitmq
from flask_smorest import Api
from swpt_creditors.flask_redis import FlaskRedis


TO_COORDINATORS_EXCHANGE = 'to_coordinators'
TO_DEBTORS_EXCHANGE = 'to_debtors'
TO_CREDITORS_EXCHANGE = 'to_creditors'
ACCOUNTS_IN_EXCHANGE = 'accounts_in'
CREDITORS_OUT_EXCHANGE = 'creditors_out'
CREDITORS_IN_EXCHANGE = 'creditors_in'
DEBTORS_OUT_EXCHANGE = 'debtors_out'
DEBTORS_IN_EXCHANGE = 'debtors_in'


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


db = CustomAlchemy()
db.signalbus.autoflush = False
migrate = Migrate()
redis_store = FlaskRedis(socket_timeout=5)
publisher = rabbitmq.Publisher(url_config_key='PROTOCOL_BROKER_URL')
api = Api()
