import six
import wrapt

from .backoff import Backoff


def backoff_factory(*args, **kwargs):
    retry_limit = kwargs.get('limit') or Backoff.limit
    retry_schedule = kwargs.get('schedule') or Backoff.schedule
    retry_random_sigma = kwargs.get('random_sigma') or Backoff.random_sigma
    retry_random_groups_per_sigma = kwargs.get('random_groups_per_sigma') or (
        Backoff.random_groups_per_sigma
    )

    class CustomBackoff(Backoff):
        schedule = retry_schedule
        limit = retry_limit
        random_sigma = retry_random_sigma
        random_groups_per_sigma = retry_random_groups_per_sigma

    return CustomBackoff


def entrypoint_retry(*args, **kwargs):
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

    :param random_sigma: integer
        Standard deviation as milliseconds. If not given,
        the default `Backoff.random_sigma` will be used.

    :param random_groups_per_sigma: integer
        Random backoffs are rounded to nearest group. If not given,
        the default `Backoff.random_groups_per_sigma` will be used.
    """
    retry_for = kwargs.get('retry_for') or Exception
    backoff_cls = backoff_factory(*args, **kwargs)

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        try:
            return wrapped(*args, **kwargs)
        except retry_for as exc:
            six.raise_from(backoff_cls(), exc)

    return wrapper
