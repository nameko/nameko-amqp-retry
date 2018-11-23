import sys

from nameko.rpc import Rpc as NamekoRpc
from nameko.rpc import RpcConsumer as NamekoRpcConsumer
from nameko_amqp_retry import Backoff, BackoffPublisher, expect_backoff_exception
from nameko_amqp_retry.constants import (
    CALL_ID_STACK_HEADER_KEY, RPC_METHOD_ID_HEADER_KEY
)


class RpcConsumer(NamekoRpcConsumer):

    def handle_message(self, body, message):

        # use the rpc_method_id if set, otherwise fall back to the routing key
        method_id = message.headers.get(RPC_METHOD_ID_HEADER_KEY)
        if method_id is None:
            method_id = message.delivery_info['routing_key']

        try:
            provider = self.get_provider_for_method(method_id)
            provider.handle_message(body, message)
        except Exception:
            exc_info = sys.exc_info()
            self.handle_result(message, None, exc_info)


class Rpc(NamekoRpc):

    rpc_consumer = RpcConsumer()
    backoff_publisher = BackoffPublisher()

    def __init__(self, *args, **kwargs):
        expected_exceptions = expect_backoff_exception(
            kwargs.pop('expected_exceptions', ())
        )
        super(Rpc, self).__init__(
            *args, expected_exceptions=expected_exceptions, **kwargs
        )

    def handle_result(self, message, worker_ctx, result, exc_info):

        if exc_info is not None:
            exc = exc_info[1]
            if isinstance(exc, Backoff):

                # add call stack and modify the current entry to show backoff
                message.headers[CALL_ID_STACK_HEADER_KEY] = (
                    worker_ctx.call_id_stack
                )
                message.headers[CALL_ID_STACK_HEADER_KEY][-1] += ".backoff"

                # when redelivering, copy the original routing key to a new
                # header so that we can still find the provider for the message
                if RPC_METHOD_ID_HEADER_KEY not in message.headers:
                    message.headers[RPC_METHOD_ID_HEADER_KEY] = (
                        message.delivery_info['routing_key']
                    )

                target_queue = "rpc-{}".format(self.container.service_name)
                try:
                    self.backoff_publisher.republish(
                        exc, message, target_queue
                    )
                    try:
                        # pylint: disable=no-member
                        self.rpc_consumer.consumer.ack_message(message)
                    except AttributeError:
                        # nameko 2.x backwards compatibilty
                        # pylint: disable=no-member
                        self.rpc_consumer.queue_consumer.ack_message(message)
                    return result, exc_info

                except Backoff.Expired:
                    exc_info = sys.exc_info()
                    result = None

        return super(Rpc, self).handle_result(
            message, worker_ctx, result=result, exc_info=exc_info
        )


rpc = Rpc.decorator
