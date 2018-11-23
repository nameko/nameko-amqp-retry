from collections import defaultdict

import pytest
from kombu import Connection
from kombu.messaging import Exchange, Queue
from kombu.pools import connections, producers
from mock import patch
from nameko.amqp.publish import get_connection
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ClusterRpcProxy
from nameko_amqp_retry import Backoff
from nameko_amqp_retry.events import event_handler
from nameko_amqp_retry.messaging import consume
from nameko_amqp_retry.rpc import rpc

BACKOFF_COUNT = 3


@pytest.yield_fixture(autouse=True)
def fast_backoff():
    value = (50, 75, 100)
    with patch.object(Backoff, 'schedule', new=value):
        yield value


@pytest.yield_fixture(autouse=True)
def no_randomness():
    with patch.object(Backoff, 'random_sigma', new=0):
        yield


@pytest.yield_fixture()
def limited_backoff():
    limit = 1
    with patch.object(Backoff, 'limit', new=limit):
        yield limit


@pytest.fixture
def exchange():
    return Exchange("messages")


@pytest.fixture
def queue(exchange):
    return Queue("messages", exchange=exchange, routing_key="message")


@pytest.fixture
def publish_message(rabbit_config):

    def publish(
        exchange, payload, routing_key=None, serializer="json", **kwargs
    ):
        conn = Connection(rabbit_config[AMQP_URI_CONFIG_KEY])

        with connections[conn].acquire(block=True) as connection:
            exchange.maybe_bind(connection)
            with producers[conn].acquire(block=True) as producer:
                producer.publish(
                    payload,
                    exchange=exchange,
                    routing_key=routing_key,
                    serializer=serializer,
                    **kwargs
                )

    return publish


@pytest.fixture
def dispatch_event(rabbit_config):

    def dispatch(service_name, event_type, event_data, **kwargs):
        dispatcher = event_dispatcher(rabbit_config, **kwargs)
        dispatcher(service_name, event_type, event_data)

    return dispatch


@pytest.yield_fixture
def rpc_proxy(rabbit_config):
    with ClusterRpcProxy(rabbit_config) as proxy:
        yield proxy


@pytest.fixture
def counter():

    class Counter(object):
        """ Counter with support for nested counts for hashable objects:

        Usage::

            counter = Counter()
            counter.increment()  # 1
            counter.increment()  # 2
            counter == 2  # True

            counter['foo'].increment()  # 1
            counter['bar'].increment()  # 1
            counter['foo'] == counter['bar'] == 1  # True

        """
        class Item(object):
            def __init__(self):
                self._value = 0

            def increment(self):
                self._value += 1
                return self._value

            def __eq__(self, other):
                return self._value == other

        sentinel = object()

        def __init__(self):
            self.items = defaultdict(Counter.Item)

        def __getitem__(self, key):
            return self.items[key]

        def increment(self):
            return self[Counter.sentinel].increment()

        def __eq__(self, other):
            return self[Counter.sentinel] == other

    return Counter()


@pytest.fixture
def backoff_count():
    return BACKOFF_COUNT


@pytest.fixture
def service_cls(queue, counter, backoff_count):

    class Service(object):
        name = "service"

        @rpc
        @consume(queue)
        @event_handler("src_service", "event_type")
        def method(self, payload):
            if counter.increment() <= backoff_count:
                raise Backoff()
            return "result"

    return Service


@pytest.fixture
def container(container_factory, rabbit_config, service_cls):

    container = container_factory(service_cls, rabbit_config)
    container.start()
    return container


@pytest.fixture
def entrypoint_tracker():

    class CallTracker(object):

        def __init__(self):
            self.calls = []

        def __len__(self):
            return len(self.calls)

        def track(self, **call):
            self.calls.append(call)

        def get_results(self):
            return [call['result'] for call in self.calls]

        def get_exceptions(self):
            return [call['exc_info'] for call in self.calls]

    return CallTracker()


@pytest.fixture
def wait_for_result(entrypoint_tracker):
    """ Callback for the `entrypoint_waiter` that waits until the entrypoint
    returns a non-exception.

    Captures the entrypoint executions using `entrypoint_tracker`.
    """
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        if exc_info is None or exc_info[0] is Backoff.Expired:
            return True
    return cb


@pytest.fixture
def wait_for_backoff_expired(entrypoint_tracker):
    """ Callback for the `entrypoint_waiter` that waits until the entrypoint
    raises a `BackoffExpired` exception.

    Captures the entrypoint executions using `entrypoint_tracker`.
    """
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        if exc_info and exc_info[0] is Backoff.Expired:
            return True
    return cb


@pytest.fixture
def queue_info(amqp_uri):
    def get_queue_info(queue_name):
        with get_connection(amqp_uri) as conn:
            queue = Queue(name=queue_name)
            queue = queue.bind(conn)
            return queue.queue_declare(passive=True)
    return get_queue_info
