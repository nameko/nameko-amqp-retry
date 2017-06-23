import itertools
import json
import time

import pytest
from kombu import Connection
from kombu.messaging import Exchange, Queue
from kombu.pools import connections
from kombu.serialization import register, unregister
from mock import ANY, patch
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.extensions import DependencyProvider
from nameko.testing.services import entrypoint_waiter
from requests.exceptions import HTTPError

from nameko_amqp_retry import Backoff, BackoffPublisher
from nameko_amqp_retry.backoff import (
    EXPIRY_GRACE_PERIOD, get_backoff_queue_name)
from nameko_amqp_retry.messaging import consume
from nameko_amqp_retry.rpc import rpc


def retry(fn):
    """ Barebones retry decorator
    """
    def wrapper(*args, **kwargs):
        exceptions = AssertionError
        max_retries = 3
        delay = 1

        counter = itertools.count()
        while True:
            try:
                return fn(*args, **kwargs)
            except exceptions:
                if next(counter) == max_retries:
                    raise
                time.sleep(delay)
    return wrapper


class QuickBackoff(Backoff):
    schedule = (100,)


class SlowBackoff(Backoff):
    schedule = (500,)


class TestPublisher(object):

    @pytest.fixture
    def container(self, container_factory, rabbit_config, queue):

        class Service(object):
            name = "service"

            @consume(queue)
            def backoff(self, delay):
                class DynamicBackoff(Backoff):
                    schedule = (delay,)
                raise DynamicBackoff()

        container = container_factory(Service, rabbit_config)
        container.start()
        return container

    def test_routing(
        self, container, publish_message, exchange, queue, counter,
        rabbit_config, rabbit_manager
    ):
        """ Queues should be dynamically created for each unique delay.
        Messages should be routed to the appropriate queue based on their
        delay value.
        """
        delays = [10000, 20000, 20000, 30000, 30000, 30000]

        def all_received(worker_ctx, res, exc_info):
            if counter.increment() == len(delays):
                return True

        # cause multiple unique backoffs to be raised
        with entrypoint_waiter(container, 'backoff', callback=all_received):
            for delay in delays:
                publish_message(exchange, delay, routing_key=queue.routing_key)

        # verify that a queue is created for each unique delay,
        # and only messages with the matching delay are in each one
        @retry
        def check_queue(delay):
            backoff_queue = rabbit_manager.get_queue(
                vhost, get_backoff_queue_name(delay)
            )
            assert backoff_queue['messages'] == delays.count(delay)

        vhost = rabbit_config['vhost']
        for delay in set(delays):
            check_queue(delay)


class TestQueueExpiry(object):

    @pytest.yield_fixture(autouse=True)
    def fast_backoff(self):
        yield

    @pytest.fixture
    def container(self, container_factory, rabbit_config, queue):

        class Service(object):
            name = "service"

            @consume(queue)
            def backoff(self, delay):
                class DynamicBackoff(Backoff):
                    schedule = (delay,)
                    limit = 1
                raise DynamicBackoff()

        container = container_factory(Service, rabbit_config)
        container.start()
        return container

    def test_queues_removed(
        self, container, publish_message, exchange, queue, counter,
        rabbit_config, rabbit_manager
    ):
        """ Backoff queues should be removed after their messages are
        redelivered.
        """
        delays = [50, 100, 100, 100, 50]

        def all_expired(worker_ctx, res, exc_info):
            if not issubclass(exc_info[0], Backoff.Expired):
                return
            if counter.increment() == len(delays):
                return True

        # cause multiple unique backoffs to be raised
        with entrypoint_waiter(container, 'backoff', callback=all_expired):
            for delay in delays:
                publish_message(exchange, delay, routing_key=queue.routing_key)

        # wait for long enough for the queues to expire
        # i.e. max(delays) + EXPIRY_GRACE_PERIOD
        time.sleep(.2)

        # verify the queues have been removed
        vhost = rabbit_config['vhost']
        for delay in set(delays):
            with pytest.raises(HTTPError) as raised:
                rabbit_manager.get_queue(
                    vhost, get_backoff_queue_name(delay)
                )
            assert raised.value.response.status_code == 404

    def test_republishing_redeclares(
        self, container, publish_message, exchange, queue, counter,
        rabbit_config, rabbit_manager
    ):
        """ Queue expiry must be reset when a new message is published to
        the backoff queue
        """
        delays = [50, 50, 50]

        def all_expired(worker_ctx, res, exc_info):
            if not issubclass(exc_info[0], Backoff.Expired):
                return
            if counter.increment() == len(delays):
                return True

        # cause multiple unique backoffs to be raised, but wait for
        # EXPIRY_GRACE_PERIOD between each publish.
        with entrypoint_waiter(container, 'backoff', callback=all_expired):
            for delay in delays:
                publish_message(exchange, delay, routing_key=queue.routing_key)
                time.sleep(EXPIRY_GRACE_PERIOD / 1000)

        # the entrypoint waiter blocks until every published message expires
        # (after exactly one backoff). if the subsequent publishes didn't
        # redeclare the queue, the later messages would be lost when the queue
        # was removed (50ms + EXPIRY_GRACE_PERIOD after the first publish)


class TestMultipleMessages(object):

    @pytest.fixture
    def container(self, container_factory, rabbit_config, counter):

        class Service(object):
            name = "service"

            backoff = BackoffPublisher()

            @rpc
            def slow(self):
                if counter["slow"].increment() <= 1:
                    raise SlowBackoff()
                return "slow"

            @rpc
            def quick(self):
                if counter["quick"].increment() <= 1:
                    raise QuickBackoff()
                return "quick"

        container = container_factory(Service, rabbit_config)
        container.start()

        return container

    def test_messages_can_leapfrog(
        self, container, entrypoint_tracker, rpc_proxy, wait_for_result
    ):
        """ Messages with short TTLs should be able to leapfrog messages with
        long TTLs that are also in the "wait" queue
        """

        # wait for both entrypoints to generate a result
        with entrypoint_waiter(
            container, 'quick', callback=wait_for_result
        ) as result_quick:
            with entrypoint_waiter(
                container, 'slow', callback=wait_for_result
            ) as result_slow:

                # wait for "slow" to fire once before calling "quick",
                # to make absolutely sure its backoff is dispatched first
                with entrypoint_waiter(container, 'slow'):
                    rpc_proxy.service.slow.call_async()
                rpc_proxy.service.quick.call_async()

        assert result_quick.get() == "quick"
        assert result_slow.get() == "slow"

        # "quick" should return a result before "slow" because it has a
        # shorter backoff interval, even though "slow" raises first
        assert entrypoint_tracker.get_results() == (
            [None, None] + ["quick", "slow"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(SlowBackoff, ANY, ANY), (QuickBackoff, ANY, ANY)] + [None, None]
        )


class TestCallStack(object):

    @pytest.fixture
    def container(self, container_factory, rabbit_config, service_cls):

        class CallStack(DependencyProvider):
            """ Exposes the call stack directly to the service
            """

            def get_dependency(self, worker_ctx):
                return worker_ctx.context_data['call_id_stack']

        class Service(service_cls):
            call_stack = CallStack()

        container = container_factory(Service, rabbit_config)
        container.start()
        return container

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_rpc_call_stack(self, container, rpc_proxy):
        """ RPC backoff extends call stack
        """
        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            rpc_proxy.service.method("msg")

        assert call_stacks == [
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1'
            ],
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1.backoff',
                'service.method.2'
            ],
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3',
            ],
            [
                'standalone_rpc_proxy.call.0',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3.backoff',
                'service.method.4'
            ],
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_events_call_stack(self, container, dispatch_event):
        """ Event handler backoff extends call stack
        """
        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            dispatch_event(
                "src_service",
                "event_type",
                {},
                headers={
                    'nameko.call_id_stack': ['event.dispatch']
                }
            )

        assert call_stacks == [
            [
                'event.dispatch',
                'service.method.0'
            ],
            [
                'event.dispatch',
                'service.method.0.backoff',
                'service.method.1'
            ],
            [
                'event.dispatch',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2',
            ],
            [
                'event.dispatch',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3'
            ],
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_messaging_call_stack(
        self, container, publish_message, exchange, queue
    ):
        """ Message consumption backoff extends call stack
        """
        call_stacks = []

        def callback(worker_ctx, result, exc_info):
            call_stacks.append(worker_ctx.call_id_stack)
            if exc_info is None:
                return True

        with entrypoint_waiter(container, 'method', callback=callback):
            publish_message(
                exchange,
                "msg",
                routing_key=queue.routing_key,
                headers={
                    'nameko.call_id_stack': ['message.publish']
                }
            )

        assert call_stacks == [
            [
                'message.publish',
                'service.method.0'
            ],
            [
                'message.publish',
                'service.method.0.backoff',
                'service.method.1'
            ],
            [
                'message.publish',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2',
            ],
            [
                'message.publish',
                'service.method.0.backoff',
                'service.method.1.backoff',
                'service.method.2.backoff',
                'service.method.3'
            ],
        ]


class TestSerialization(object):

    @pytest.yield_fixture(autouse=True)
    def custom_serializer(self, rabbit_config):

        def encode(value):
            value = json.dumps(value)
            return value.upper()

        def decode(value):
            value = value.lower()
            return json.loads(value)

        # register new serializer
        register(
            "upperjson", encode, decode, "application/x-upper-json", "utf-8"
        )
        # update config so consumers expect it
        rabbit_config['serializer'] = "upperjson"
        yield
        unregister("upperjson")

    def test_custom_serialization(
        self, container, publish_message, exchange, queue, wait_for_result
    ):
        """ Backoff can be used with a custom AMQP message serializer
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            publish_message(
                exchange,
                "msg",
                serializer="upperjson",
                routing_key=queue.routing_key
            )

        assert result.get() == "result"


class TestDeadLetteredMessages(object):

    @pytest.yield_fixture(autouse=True)
    def limited_backoff(self, backoff_count):
        # allow exactly `backoff_count` backoffs
        limit = backoff_count
        with patch.object(Backoff, 'limit', new=limit):
            yield limit

    @pytest.fixture
    def deadlettering_exchange(self, rabbit_config, exchange, queue):
        conn = Connection(rabbit_config[AMQP_URI_CONFIG_KEY])

        with connections[conn].acquire(block=True) as connection:

            deadletter_exchange = Exchange(name="deadletter", type="topic")
            deadletter_exchange.maybe_bind(connection)
            deadletter_exchange.declare()

            deadletter_queue = Queue(
                name="deadletter",
                exchange=deadletter_exchange,
                routing_key="#",
                queue_arguments={
                    'x-dead-letter-exchange': exchange.name
                }
            )
            deadletter_queue.maybe_bind(connection)
            deadletter_queue.declare()

        return deadletter_exchange

    def test_backoff_works_on_previously_deadlettered_message(
        self, container, publish_message, deadlettering_exchange,
        queue, exchange, wait_for_result, entrypoint_tracker, limited_backoff
    ):
        """ Backoff can be used even if the original message has previously
        been deadlettered
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            # dispatch a message to the deadlettering exchange.
            # it will be deadlettered into the normal `exchange`
            # and should afterwards be processed as "normal" message
            publish_message(
                deadlettering_exchange,
                "msg",
                routing_key=queue.routing_key,
                expiration=1.0
            )

        # the initial deadlettering should not count towards the backoff limit,
        # so we shouldn't see Backoff.Expired here
        assert result.get() == "result"

        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + ["result"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff + [None]
        )
