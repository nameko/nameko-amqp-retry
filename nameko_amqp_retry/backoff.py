import random

import six
from kombu import Connection
from kombu.common import maybe_declare
from kombu.messaging import Exchange, Queue
from kombu.pools import connections, producers
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.extensions import SharedExtension


def get_backoff_queue_name(expiration):
    return "backoff--{}".format(expiration)


class Backoff(Exception):

    schedule = (1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000)
    randomness = 100  # standard deviation as milliseconds
    limit = 20

    class Expired(Exception):
        pass

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

    def get_next_expiration(self, message, backoff_exchange_name):

        total_attempts = 0
        for deadlettered in message.headers.get('x-death', ()):
            if deadlettered['exchange'] == backoff_exchange_name:
                total_attempts = int(deadlettered['count'])
                break

        if self.limit and total_attempts >= self.limit:
            expired = Backoff.Expired(
                "Backoff aborted after '{}' retries (~{:.0f} seconds)".format(
                    self.limit, self.max_delay / 1000
                )
            )
            six.raise_from(expired, self)

        expiration = self.get_next_schedule_item(total_attempts)

        if self.randomness:
            expiration = int(random.gauss(expiration, self.randomness))
        return expiration


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
                'x-dead-letter-exchange': ""   # default exchange
            }
        )
        return backoff_queue

    def republish(self, backoff_exc, message, target_queue):

        expiration = backoff_exc.get_next_expiration(
            message, self.exchange.name
        )
        queue = self.make_queue(expiration)

        # republish to appropriate backoff queue
        conn = Connection(self.container.config[AMQP_URI_CONFIG_KEY])
        with connections[conn].acquire(block=True) as connection:

            maybe_declare(self.exchange, connection)
            maybe_declare(queue, connection)

            with producers[conn].acquire(block=True) as producer:

                properties = message.properties.copy()
                headers = properties.pop('application_headers')

                headers['backoff'] = expiration
                expiration_seconds = float(expiration) / 1000

                producer.publish(
                    message.body,
                    headers=headers,
                    exchange=self.exchange,
                    routing_key=target_queue,
                    expiration=expiration_seconds,
                    **properties
                )
