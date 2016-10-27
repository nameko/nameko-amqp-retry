from nameko.events import EventHandler as NamekoEventHandler

from nameko_amqp_retry.messaging import Consumer


class EventHandler(NamekoEventHandler, Consumer):
    pass


event_handler = EventHandler.decorator
