import random
from pkg_resources import parse_version

import six
from kombu import Connection, __version__ as kombu_version
from kombu.common import maybe_declare
from kombu.messaging import Exchange, Queue
from nameko.amqp.publish import Publisher
from nameko.constants import AMQP_URI_CONFIG_KEY, DEFAULT_RETRY_POLICY
from nameko.extensions import SharedExtension

EXPIRY_GRACE_PERIOD = 5000  # ms


KOMBU_PRE_4_3 = parse_version(kombu_version) < parse_version('4.3.0')


def get_backoff_queue_name(expiration):
    return "backoff--{}ms".format(expiration)


def round_to_nearest(value, interval):
    return int(interval * round(float(value) / interval))


class Backoff(Exception):

    schedule = (1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000)
    limit = 20

    random_sigma = 100
    # standard deviation as milliseconds

    random_groups_per_sigma = 5
    # random backoffs are rounded to nearest group

    class Expired(Exception):
        pass

    def __init__(self, *args):
        super(Backoff, self).__init__(*args)
        self._total_attempts = None
        self._next_expiration = None

    @property
    def max_delay(self):
        return sum(
            self.get_next_schedule_item(index) for index in range(self.limit)
        )

    @classmethod
    def get_next_schedule_item(cls, index):
        if index >= len(cls.schedule):
            item = cls.schedule[-1]
        else:
            item = cls.schedule[index]
        return item

    def next(self, message, backoff_exchange_name):

        total_attempts = 0
        for deadlettered in message.headers.get('x-death', ()):
            if deadlettered['exchange'] == backoff_exchange_name:
                total_attempts += int(deadlettered['count'])

        if self.limit and total_attempts >= self.limit:
            expired = Backoff.Expired(
                "Backoff aborted after '{}' retries (~{:.0f} seconds)".format(
                    self.limit, self.max_delay / 1000
                )
            )
            six.raise_from(expired, self)

        expiration = self.get_next_schedule_item(total_attempts)

        if self.random_sigma:
            randomised = int(random.gauss(expiration, self.random_sigma))
            group_size = self.random_sigma / self.random_groups_per_sigma
            expiration = round_to_nearest(randomised, interval=group_size)

        # Prevent any negative values created by randomness
        expiration = abs(expiration)

        # store calculation results on self.
        self._next_expiration = expiration
        self._total_attempts = total_attempts

        return expiration

    def __str__(self):
        return 'Backoff({})'.format(
            'retry #{} in {}ms'.format(
                self._total_attempts + 1, self._next_expiration)
            if self._next_expiration is not None
            else 'uninitialised'
        )


class BackoffPublisher(SharedExtension):

    @property
    def exchange(self):
        backoff_exchange = Exchange(
            type="headers",
            name="backoff"
        )
        return backoff_exchange

    def make_queue(self, expiration):
        backoff_queue = Queue(
            name=get_backoff_queue_name(expiration),
            exchange=self.exchange,
            binding_arguments={
                'backoff': expiration,
                'x-match': 'any'
            },
            queue_arguments={
                'x-expires': expiration + EXPIRY_GRACE_PERIOD,
                'x-dead-letter-exchange': ""   # default exchange
            }
        )
        return backoff_queue

    def republish(self, backoff_exc, message, target_queue):

        expiration = backoff_exc.next(message, self.exchange.name)
        queue = self.make_queue(expiration)

        properties = message.properties.copy()
        headers = properties.pop('application_headers', {})

        headers['backoff'] = expiration
        expiration_seconds = float(expiration) / 1000

        amqp_uri = self.container.config[AMQP_URI_CONFIG_KEY]

        # force redeclaration;
        # In kombu versions prior to 4.3.0, the publisher will skip declaration if
        # the entity has previously been declared by the same connection.
        # (see https://github.com/celery/kombu/pull/884)
        conn = Connection(amqp_uri)
        if KOMBU_PRE_4_3:  # pragma: no cover
            maybe_declare(
                queue, conn.channel(), retry=True, **DEFAULT_RETRY_POLICY
            )

        # republish to appropriate backoff queue
        publisher = Publisher(amqp_uri)
        publisher.publish(
            message.body,
            headers=headers,
            exchange=self.exchange,
            routing_key=target_queue,
            expiration=expiration_seconds,
            mandatory=True,
            retry=True,
            retry_policy=DEFAULT_RETRY_POLICY,
            declare=[queue.exchange, queue],
            **properties
        )
