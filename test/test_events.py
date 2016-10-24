import traceback

import pytest
import six
from mock import ANY
from nameko.testing.services import entrypoint_waiter

from nameko_amqp_retry import Backoff
from nameko_amqp_retry.events import event_handler

from test import PY3


class TestEvents(object):

    def test_events(
        self, container, entrypoint_tracker, dispatch_event, wait_for_result,
        backoff_count
    ):
        """ Event handler supports backoff
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_result
        ) as result:
            dispatch_event("src_service", "event_type", {})

        assert result.get() == "result"

        assert entrypoint_tracker.get_results() == (
            [None] * backoff_count + ["result"]
        )
        assert entrypoint_tracker.get_exceptions() == (
            [(Backoff, ANY, ANY)] * backoff_count + [None]
        )

    def test_expiry(
        self, container, entrypoint_tracker, dispatch_event, limited_backoff,
        wait_for_backoff_expired
    ):
        """ Event handler supports backoff expiry
        """
        with entrypoint_waiter(
            container, 'method', callback=wait_for_backoff_expired
        ) as result:
            dispatch_event("src_service", "event_type", {})

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
        self, container_factory, rabbit_config, backoff_count, dispatch_event,
        counter, entrypoint_tracker, wait_for_result
    ):
        """ Backoff can be chained to a root-cause exception
        """
        class NotYet(Exception):
            pass

        class Service(object):
            name = "service"

            @event_handler("src_service", "event_type")
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
            dispatch_event("src_service", "event_type", {})

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
        self, container_factory, rabbit_config, counter, dispatch_event,
        limited_backoff, entrypoint_tracker, wait_for_backoff_expired,
    ):
        """ Backoff.Expired can be chained to a Backoff exception and
        root-cause exception
        """
        class NotYet(Exception):
            pass

        class Service(object):
            name = "service"

            @event_handler("src_service", "event_type")
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
            dispatch_event("src_service", "event_type", {})

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

    def test_multiple_services(
        self, dispatch_event, container_factory, entrypoint_tracker,
        backoff_count, rabbit_config, counter, wait_for_result, rabbit_manager
    ):
        """ Event handler backoff works when multiple services use it
        """
        class ServiceOne(object):
            name = "service_one"

            @event_handler("src_service", "event_type")
            def method(self, payload):
                if counter["one"].increment() <= backoff_count:
                    raise Backoff()
                return "one"

        class ServiceTwo(object):
            name = "service_two"

            @event_handler("src_service", "event_type")
            def method(self, payload):
                if counter["two"].increment() <= backoff_count:
                    raise Backoff()
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

                dispatch_event("src_service", "event_type", {})

        assert result_one.get() == "one"
        assert result_two.get() == "two"

        assert counter['one'] == backoff_count + 1
        assert counter['two'] == backoff_count + 1

        results = entrypoint_tracker.get_results()
        # order not guaranteed
        assert results.count(None) == backoff_count * 2
        assert results.count("one") == results.count("two") == 1

    def test_multiple_handlers(
        self, container_factory, rabbit_config, wait_for_result,
        entrypoint_tracker, dispatch_event, counter, backoff_count
    ):
        """ Event handler backoff works when multiple entrypoints in the same
        service use it, including events with identical types originating from
        different services.
        """
        class Service(object):
            name = "service"

            @event_handler("s1", "e1")
            def a(self, payload):
                if counter["a"].increment() <= backoff_count:
                    raise Backoff()
                return "a"

            @event_handler("s1", "e2")
            def b(self, payload):
                if counter["b"].increment() <= backoff_count:
                    raise Backoff()
                return "b"

            @event_handler("s2", "e1")
            def c(self, payload):
                if counter["c"].increment() <= backoff_count:
                    raise Backoff()
                return "c"

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_waiter(container, 'a', callback=wait_for_result):
            dispatch_event('s1', 'e1', {})
        assert entrypoint_tracker.get_results() == (
            [None] * backoff_count + ["a"]
        )
        assert counter['a'] == backoff_count + 1

        with entrypoint_waiter(container, 'b', callback=wait_for_result):
            dispatch_event('s1', 'e2', {})
        assert entrypoint_tracker.get_results() == (
            [None] * backoff_count + ["a"] +
            [None] * backoff_count + ["b"]
        )
        assert counter['b'] == backoff_count + 1

        with entrypoint_waiter(container, 'c', callback=wait_for_result):
            dispatch_event('s2', 'e1', {})
        assert entrypoint_tracker.get_results() == (
            [None] * backoff_count + ["a"] +
            [None] * backoff_count + ["b"] +
            [None] * backoff_count + ["c"]
        )
        assert counter['c'] == backoff_count + 1
