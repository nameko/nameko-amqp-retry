nameko-amqp-retry
=================

Extension for `nameko <http://nameko.readthedocs.org>`_ that allows the built-in AMQP entrypoints to schedule a later retry via redelivery of a message.

It depends on the *experimental* `delayed message exchange <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange>`_ plugin for RabbitMQ.

Installation
------------

The delayed message exchange plugin should be installed following `their instructions <https://github.com/rabbitmq/rabbitmq-delayed-message-exchange#installing>`_.

No further configuration is required. The "backoff" exchange and queue used by this library will be created on demand.

Usage
-----

Raise :class:`nameko_amqp_retry.Backoff` inside the entrypoint you wish to retry later:

.. code-block:: python

    from nameko_amqp_retry import Backoff

    class Service:
        name = "service"

        @rpc
        def calculate(self):
            """ Calculate something, or schedule a retry if not ready yet.
            """
            if not_ready_yet:
                raise Backoff()

            return 42

The caller will see the final result, or a :class:`Backoff.Expired` exception if more than the allowed number of retries were made:

.. code-block:: python

    >>> n.rpc.service.calculate()
    ... # blocks for some time
    >>> 42

.. code-block:: python

    >>> n.rpc.service.calculate()
    Traceback (most recent call last):
      ...
    nameko.exceptions.RemoteError: Expired Backoff aborted after ...

The retry schedule is controlled by attributes on the `Backoff` class. You should override them in a subclass to control the behaviour. For example:

Fixed schedule:

.. code-block:: python

    class RegularBackoff(Backoff):
        """ Retries every 1000ms until limit
        """
        schedule = (1000,)  # ms

No limit:

.. code-block:: python

    class InfiniteBackoff(Backoff):
        """ Retries forever
        """
        limit = 0


Custom schedule:

.. code-block:: python

    class ImpatientBackoff(Backoff):
        """ Retries after 100, then 200, then 500 milliseconds
        """
        schedule = (100, 200, 500)  # ms


Dynamic schedule:

.. code-block:: python

    class DynamicBackoff(Backoff):
        """ Calculates schedule dynamically
        """
        @classmethod
        def get_next_schedule_item(cls, index):
            ...


See docs/examples for more.
