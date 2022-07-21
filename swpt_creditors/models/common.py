from __future__ import annotations
from datetime import datetime, timezone
import dramatiq
from swpt_creditors.extensions import db, publisher
from flask_signalbus import rabbitmq

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
ROOT_CREDITOR_ID = 0
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

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        assert(all(isinstance(obj, cls) for obj in objects))
        messages = [obj._create_message() for obj in objects]
        publisher.publish_messages(messages)

    def send_signalbus_message(self):  # pragma: no cover
        self.send_signalbus_messages([self])

    def _create_message(self):  # pragma: no cover
        data = self.__marshmallow_schema__.dump(self)
        dramatiq_message = dramatiq.Message(
            queue_name=None,
            actor_name=self.actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        headers = {
            'debtor-id': data['debtor_id'],
            'creditor-id': data['creditor_id'],
        }
        if 'coordinator_id' in data:
            headers['coordinator-id'] = data['coordinator_id']
            headers['coordinator-type'] = data['coordinator_type']
        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id='swpt_creditors',
            content_type='application/json',
            type=self.message_type,
            headers=headers,
        )
        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=dramatiq_message.encode(),
            properties=properties,
            mandatory=True,
        )

    inserted_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
