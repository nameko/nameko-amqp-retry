import re

import arrow
from kombu import Connection
from kombu.pools import connections, producers

from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import entrypoint_waiter


def publisher(nameko_config, **kwargs):
    """ Return a function that sends AMQP messages.
    """
    def publish(payload, routing_key, exchange=None):
        """ Dispatch a message with `payload`
        """
        conn = Connection(nameko_config[AMQP_URI_CONFIG_KEY])

        with connections[conn].acquire(block=True) as connection:
            if exchange is not None:  # pragma: no cover
                exchange.maybe_bind(connection)
            with producers[conn].acquire(block=True) as producer:
                producer.publish(
                    payload,
                    routing_key=routing_key,
                    exchange=exchange,
                    **kwargs
                )
    return publish


def wait_for_result(worker_ctx, res, exc_info):
    return exc_info is None


class TestBackoff(object):

    def test_rpc(self, container_factory, rabbit_config):

        from examples.retry import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        timestamp = arrow.utcnow().replace(seconds=+1)

        with ServiceRpcProxy('service', rabbit_config) as service_rpc:
            res = service_rpc.method(timestamp.isoformat())
        assert arrow.get(re.match("Time is (.+)", res).group(1)) >= timestamp

    def test_decorated_rpc(self, container_factory, rabbit_config):

        from examples.retry import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        timestamp = arrow.utcnow().replace(seconds=+1)

        with ServiceRpcProxy('service', rabbit_config) as service_rpc:
            res = service_rpc.decorated_method(timestamp.isoformat())
        assert arrow.get(re.match("Time is (.+)", res).group(1)) >= timestamp

    def test_event(self, container_factory, rabbit_config):

        from examples.retry import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        timestamp = arrow.utcnow().replace(seconds=+1)

        dispatch = event_dispatcher(rabbit_config)
        with entrypoint_waiter(
            container, 'handle_event', callback=wait_for_result
        ) as result:
            payload = {'timestamp': timestamp.isoformat()}
            dispatch("src_service", "event_type", payload)

        res = result.get()
        assert arrow.get(re.match("Time is (.+)", res).group(1)) >= timestamp

    def test_message(self, container_factory, rabbit_config):

        from examples.retry import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        timestamp = arrow.utcnow().replace(seconds=+1)

        publish = publisher(rabbit_config)
        with entrypoint_waiter(
            container, 'handle_message', callback=wait_for_result
        ) as result:
            payload = {'timestamp': timestamp.isoformat()}
            publish(payload, routing_key="messages")

        res = result.get()
        assert arrow.get(re.match("Time is (.+)", res).group(1)) >= timestamp
