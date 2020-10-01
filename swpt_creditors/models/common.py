from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone
import dramatiq
from swpt_creditors.extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
TS0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
DATE0 = TS0.date()
PIN_REGEX = r'^[0-9]{4,10}$'
TRANSFER_NOTE_MAX_BYTES = 500
TRANSFER_NOTE_FORMAT_REGEX = r'^[0-9A-Za-z.-]{0,8}$'
CT_DIRECT = 'direct'


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models. Make sure
    #      TTL is set properly for the messages.

    queue_name: Optional[str] = None

    @property
    def event_name(self):  # pragma: no cover
        model = type(self)
        return f'on_{model.__tablename__}'

    def send_signalbus_message(self):  # pragma: no cover
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.actor_name is set, but SignalModel.queue_name is not'
            actor_name = self.event_name
            routing_key = f'events.{actor_name}'
        else:
            actor_name = model.actor_name
            routing_key = model.queue_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        broker.publish_message(message, exchange=MAIN_EXCHANGE_NAME, routing_key=routing_key)

    inserted_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
