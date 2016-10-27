import traceback

import pytest
import six
from kombu.messaging import Queue
from mock import ANY
from nameko.testing.services import entrypoint_waiter

from nameko_amqp_retry import Backoff
from nameko_amqp_retry.messaging import consume

from test import PY3


class TestMessaging(object):

    @pytest.mark.usefixtures('container')
    def test_messaging(
        self, container, entrypoint_tracker, publish_message, exchange, queue,
        wait_for_result, backoff_count
    ):
        """ Message consumption supports backoff
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        assert result.get() == "result"

        assert entrypoint_tracker.get_results() == (
            [None] * backoff_count + ["result"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * backoff_count + [None]
        )

    def test_expiry(
        self, container, entrypoint_tracker, publish_message, exchange, queue,
        limited_backoff, wait_for_backoff_expired
    ):
        """ Message consumption supports backoff expiry
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + [None]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff +
            [(Backoff.Expired, ANY, ANY)]
        )

    def test_chain_backoff_exception(
        self, container_factory, rabbit_config, queue, exchange, backoff_count,
        counter, entrypoint_tracker, wait_for_result, publish_message
    ):
        """ Backoff can be chained to a root-cause exception
        """
        class NotYet(Exception):
            pass

        class Service(object):
            name = "service"

            @consume(queue)
            def method(self, arg):
                try:
                    if counter.increment() <= backoff_count:
                        raise NotYet("try again later")
                except NotYet as exc:
                    six.raise_from(Backoff(), exc)
                return "result"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        assert result.get() == "result"

        # entrypoint fired backoff_count + 1 times
        assert entrypoint_tracker.get_results() == (
            [None] * backoff_count + ["result"]
        )
        # entrypoint raised `Backoff` for all but the last execution
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * backoff_count + [None]
        )

        # on py3, backoff contains chained exception
        if PY3:
            exc_type, exc, tb = entrypoint_tracker.get_exceptions()[0]
            stack = "".join(traceback.format_exception(exc_type, exc, tb))
            assert "NotYet: try again later" in stack
            assert "nameko_amqp_retry.backoff.Backoff" in stack

    def test_chain_backoff_expired(
        self, container_factory, rabbit_config, queue, exchange, counter,
        limited_backoff, entrypoint_tracker, wait_for_backoff_expired,
        publish_message
    ):
        """ Backoff.Expired can be chained to a Backoff exception and
        root-cause exception
        """
        class NotYet(Exception):
            pass

        class Service(object):
            name = "service"

            @consume(queue)
            def method(self, arg):
                try:
                    raise NotYet("try again later")
                except NotYet as exc:
                    six.raise_from(Backoff(), exc)
                return "result"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        with pytest.raises(Backoff.Expired) as raised:
            result.get()
        assert (
            "Backoff aborted after '{}' retries".format(limited_backoff)
        ) in str(raised.value)

        # entrypoint fired `limited_backoff` + 1 times
        assert entrypoint_tracker.get_results() == (
            [None] * limited_backoff + [None]
        )
        # entrypoint raised `Backoff` for all but the last execution,
        # and then raised `Backoff.Expired`
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * limited_backoff +
            [(Backoff.Expired, ANY, ANY)]
        )

        # on py3, backoff expired contains chained exceptions
        if PY3:
            exc_type, exc, tb = entrypoint_tracker.get_exceptions()[-1]
            stack = "".join(traceback.format_exception(exc_type, exc, tb))
            assert "NotYet: try again later" in stack
            assert "nameko_amqp_retry.backoff.Backoff" in stack
            assert "nameko_amqp_retry.backoff.Expired" in stack

    def test_multiple_queues_with_same_exchange_and_routing_key(
        self, container_factory, entrypoint_tracker, rabbit_manager, exchange,
        wait_for_result, publish_message, counter, rabbit_config, backoff_count
    ):
        """ Message consumption backoff works when there are muliple queues
        receiving the published message
        """
        queue_one = Queue("one", exchange=exchange, routing_key="message")
        queue_two = Queue("two", exchange=exchange, routing_key="message")

        class ServiceOne(object):
            name = "service_one"

            @consume(queue_one)
            def method(self, payload):
                if counter["one"].increment() <= backoff_count:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @consume(queue_two)
            def method(self, payload):
                counter["two"].increment()
                return "two"

        container_one = container_factory(ServiceOne, rabbit_config)
        container_one.start()
        container_two = container_factory(ServiceTwo, rabbit_config)
        container_two.start()

        with entrypoint_waiter(
            container_one, 'method', callback=wait_for_result
        ) as result_one:

            with entrypoint_waiter(
                container_two, 'method', callback=wait_for_result
            ) as result_two:

                publish_message(exchange, "msg", routing_key="message")

        # ensure all messages are processed
        vhost = rabbit_config['vhost']
        backoff_queue = rabbit_manager.get_queue(vhost, 'backoff')
        service_queue_one = rabbit_manager.get_queue(vhost, queue_one.name)
        service_queue_two = rabbit_manager.get_queue(vhost, queue_two.name)
        assert backoff_queue['messages'] == 0
        assert service_queue_one['messages'] == 0
        assert service_queue_two['messages'] == 0

        assert result_one.get() == "one"
        assert result_two.get() == "two"

        # backoff from service_one not seen by service_two
        assert counter['one'] == backoff_count + 1
        assert counter['two'] == 1

    def test_non_backoff_exception(
        self, container_factory, rabbit_config, publish_message,
        queue, exchange
    ):
        """ Non-backoff exceptions are handled normally
        """
        class Boom(Exception):
            pass

        class Service(object):
            name = "service"

            @consume(queue)
            def handle(self, payload):
                raise Boom()

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(container, 'handle') as result:
            publish_message(exchange, "msg", routing_key=queue.routing_key)

        with pytest.raises(Boom):
            result.get()
