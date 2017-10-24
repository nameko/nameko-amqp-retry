from .backoff import Backoff, BackoffPublisher  # noqa
from .decorators import entrypoint_retry  # noqa


def expect_backoff_exception(expected_exceptions):
    if not issubclass(Backoff, expected_exceptions):
        try:
            expected_exceptions = expected_exceptions + (Backoff,)
        except TypeError:
            expected_exceptions = (expected_exceptions, Backoff)
    return expected_exceptions
