nameko-amqp-retry
=================

Extension for `nameko <http://nameko.readthedocs.org>`_ that allows the built-in AMQP entrypoints to schedule a later retry via redelivery of a message.

RabbitMQ 3.5.4 or later is required.


Installation
------------

Install the library from PyPI::

    pip install nameko-amqp-retry


Usage
-----

This library subclasses nameko's built-in entrypoints. Use these subclasses in your service definition, and then raise :class:`nameko_amqp_retry.Backoff` inside an entrypoint you wish to retry later:

.. code-block:: python

    from nameko_amqp_retry import Backoff
    from nameko_amqp_retry.rpc import rpc

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

Alternatively, an entrypoint can be decorated with the `entrypoint_retry` decorator, to automatically retry the method if it raises certain exceptions:

.. code-block:: python

    from nameko_amqp_retry import entrypoint_retry
    from nameko_amqp_retry.rpc import rpc

    class Service:
        name = "service"

        @rpc
        @entrypoint_retry(retry_for=ValueError)
        def calculate(self):
            """ Calculate something, or schedule a retry if not ready yet.
            """
            if not_ready_yet:
                raise ValueError()

            return 42

        @rpc
        @entrypoint_retry(
            retry_for=(TypeError, ValueError),
            limit=5,
            schedule=(500, 600, 700, 800, 900),
        )
        def do_something(self):
            """ Calculate something else, or schedule a retry if not ready yet.
            """
            if type_not_ready_yet:
                raise TypeError()

            if value_not_ready_yet:
                raise ValueError()

            return 24


See docs/examples for more.
