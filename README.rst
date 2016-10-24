nameko-amqp-retry
=================

Extension for `nameko <http://nameko.readthedocs.org>`_ that allows the built-in AMQP entrypoints to schedule a later retry via redelivery of a message.

It depends on the *experimental* `delayed message exchange <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange>`_ plugin for RabbitMQ.

Installation
------------

The delayed message exchange plugin should be installed following `their instructions <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange#installing their instructions>`_.

No further configuration is required. The "backoff" exchange and queue used by this library will be created on demand.

Usage
-----

Raise `nameko_amqp_retry.Backoff` inside the entrypoint you wish to retry later::

    @rpc
    def calculate(self):
        """ Calculate something, or schedule a retry if not ready yet.
        """
        if not_ready_yet:
            raise Backoff()

        return 42

The caller will see the final result, or a `Backoff.Expired` exception if more than the allowed number of retries were made:

::
    >>> n.rpc.calculate()
    ... # blocks for some time
    >>> 42

::
    >>> n.rpc.calculate()
    ... # blocks for some time
    >>> Traceback: Backoff.Expired
        ...

The retry schedule is controlled by attributes on the `Backoff` class. You should override them in a subclass to control the behaviour. For example:

Fixed schedule:

::
    class RegularBackoff(Backoff):
        """ Retries every 1000ms until limit
        """
        schedule = (1000,)  # ms

No limit:

::
    class InfiniteBackoff(Backoff):
        """ Retries forever
        """
        limit = None


Custom schedule:

::
    class ImpatientBackoff(Backoff):
        """ Retries after 100, then 200, then 500 milliseconds
        """
        schedule = (100, 200, 500)  # ms


Dynamic schedule:

::
    class DynamicBackoff(Backoff):
        """ Calculates schedule dynamically
        """
        @classmethod
        def get_next_schedule_item(cls, index):
            ...

See docs/examples for more.
