import arrow
from kombu.messaging import Queue

from nameko_amqp_retry import Backoff, entrypoint_retry
from nameko_amqp_retry.events import event_handler
from nameko_amqp_retry.messaging import consume
from nameko_amqp_retry.rpc import rpc


class Service:
    name = "service"

    def generate_message(self):
        return "Time is {}".format(arrow.utcnow())

    @rpc
    def method(self, timestamp):
        """ Return a message on or after `timestamp`.

        The method will be called repeatedly until `timestamp` has passed.
        """
        if arrow.get(timestamp) < arrow.utcnow():
            return self.generate_message()

        raise Backoff()

    @event_handler('src_service', 'event_type')
    def handle_event(self, event_data):
        """ Print a message on or after `event_data['timestamp']`

        The event will be redelivered repeatedly until `timestamp` has passed.
        """
        timestamp = event_data.get('timestamp')
        if arrow.get(timestamp) < arrow.utcnow():
            msg = self.generate_message()
            print(msg)
            return msg

        raise Backoff()

    @consume(Queue('messages'))
    def handle_message(self, payload):
        """ Print a message on or after `payload['timestamp']`

        The message will be redelivered repeatedly until `timestamp` has
        passed.
        """
        timestamp = payload.get('timestamp')
        if arrow.get(timestamp) < arrow.utcnow():
            msg = self.generate_message()
            print(msg)
            return msg

        raise Backoff()

    @rpc
    @entrypoint_retry(retry_for=ValueError)
    def decorated_method(self, timestamp):
        """ Return a message on or after `timestamp`.

        The method will be called repeatedly until `timestamp` has passed.
        """
        if arrow.get(timestamp) < arrow.utcnow():
            return self.generate_message()

        raise ValueError()
