import six
import wrapt

from .backoff import Backoff


def backoff_factory(limit=None, schedule=None):
    retry_limit = Backoff.limit if limit is None else limit
    retry_schedule = Backoff.schedule if schedule is None else schedule

    class CustomBackoff(Backoff):
        schedule = retry_schedule
        limit = retry_limit

    return CustomBackoff


def entrypoint_retry(retry_for=Exception, limit=None, schedule=None):
    """
    Decorator to declare that an entrypoint can be retried on failure.

    For use with nameko_amqp_retry enabled entrypoints.

    :param retry_for: An exception class or tuple of exception classes.
        If the wrapped function raises one of these exceptions, the entrypoint
        will be retried until successful, or the `limit` number of retries
        is reached.

    :param limit: integer
         The maximum number of times the entrypoint can be retried before
         giving up (and raising a ``Backoff.Expired`` exception).
         If not given, the default `Backoff.limit` will be used.

    :param schedule: tuple of integers
        A tuple defining the number of milliseconds to wait between each
        retry. If not given, the default `Backoff.schedule` will be used.
    """
    backoff_cls = backoff_factory(limit=limit, schedule=schedule)

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        try:
            return wrapped(*args, **kwargs)
        except retry_for as exc:
            six.raise_from(backoff_cls(), exc)

    return wrapper
