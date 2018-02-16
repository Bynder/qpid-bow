import asyncio
import time
from logging import getLogger

from typing import List, Union

from proton import Connection, Receiver, Sender, Session, Url
from proton.reactor import Container as BaseContainer, LinkOption
from proton.handlers import IOHandler

logger = getLogger()


class AsyncioReactorHandler:
    """Qpid Proton Reactor Global Loop Handler for Python asyncio.

    This implementation will setup Qpid Proton's Selectables to use asyncio's
    writable/readable event handling.

    Based on Tornado implementation:
    https://qpid.apache.org/releases/qpid-proton-0.18.1/proton/python/examples/proton_tornado.py.html

    Args:
        loop: An asyncio event loop
        handler_base: An IO Handler
    """

    def __init__(self, loop=None, handler_base=None):
        self.loop = loop or asyncio.get_event_loop()
        self.io = handler_base or IOHandler()
        self._count = 0
        self._reactor = None

    def on_reactor_init(self, event):
        logger.debug("Reactor initted")
        self._reactor = event.reactor

    def on_reactor_quiesced(self, event):  # pylint: disable=no-self-use
        # check if we are still quiesced, other handlers of
        # on_reactor_quiesced could have produced events to process
        if not event.reactor.quiesced:
            return

        logger.debug("Reactor quiesced")
        event.reactor.yield_()

    def on_unhandled(self, name, event):  # pylint: disable=unused-argument
        event.dispatch(self.io)

    def _schedule(self, selectable):
        if selectable.deadline:
            logger.debug("Setting up schedule for %s", selectable)
            self.loop.call_later(
                selectable.deadline - time.time(),
                lambda: self._scheduled_selectable_expired(selectable))

    def _scheduled_selectable_expired(self, selectable):
        logger.debug("Scheduled selectable %s expired", selectable)
        selectable.expired()
        self._process()

    def _process(self):
        self._reactor.process()
        if not self._reactor.quiesced:
            self.loop.call_soon_threadsafe(self._process)

    def _selectable_readable(self, selectable):
        logger.debug("Readable callback: %s", selectable)
        selectable.readable()
        self._process()

    def _selectable_writable(self, selectable):
        logger.debug("Writable callback: %s", selectable)
        selectable.writable()
        self._process()

    def _setup_selectable(self, selectable):
        if selectable.reading:
            logger.debug("Setting up reader for %s", selectable)
            self.loop.add_reader(
                selectable.fileno(), self._selectable_readable, selectable)
        if selectable.writing:
            logger.debug("Setting up writer for %s", selectable)
            self.loop.add_writer(
                selectable.fileno(), self._selectable_writable, selectable)

    def _teardown_selectable(self, selectable):
        logger.debug("Resetting %s", selectable)
        self.loop.remove_writer(selectable.fileno())
        self.loop.remove_reader(selectable.fileno())

    def on_selectable_init(self, event):
        selectable = event.context
        if selectable.fileno() >= 0:
            self._setup_selectable(selectable)

        self._schedule(selectable)
        self._count += 1

    def on_selectable_updated(self, event):
        selectable = event.context
        if selectable.fileno() > 0:
            self._teardown_selectable(selectable)
            self._setup_selectable(selectable)

        self._schedule(selectable)

    def on_selectable_final(self, event):
        selectable = event.context
        if selectable.fileno() > 0:
            self._teardown_selectable(selectable)

        logger.debug("Selectable final %s", selectable)
        selectable.release()
        self._count -= 1
        if self._count == 0:
            self.loop.call_soon_threadsafe(self._stop)

    def _stop(self):
        if self._reactor:
            logger.debug("Stopping reactor")
            self._reactor.stop()


# Needs to be called Container for magic reasons :(
class Container(BaseContainer):
    """Asyncio event loop based Qpid Reactor container.

    Args:
        *handlers: One or more connectors

    Keyword Args:
        handler_base: An IO Handler.
        impl: Reactor implementation, default is pn_reactor.
    """
    def __init__(self, *handlers, **kwargs):
        loop = kwargs.get('loop', asyncio.get_event_loop())
        kwargs['global_handler'] = AsyncioReactorHandler(
            loop, kwargs.get('handler_base', None))
        super().__init__(*handlers, **kwargs)
        self.loop = loop

    def run(self):
        """Start Reactor container and begin processing."""
        self.start()
        self.touch()

    def touch(self):
        """Instruct the reactor container to do processing.

        You might need to call this to startup new sessions. This is already
        handled for create_receiver and create_sender.
        """
        if self.process() and not self.quiesced:
            self.loop.call_soon_threadsafe(self.touch)

    def create_receiver(
            self, context: Union[Connection, Session, Url, str],
            source=None, target=None, name=None, dynamic=False, handler=None,
            options: Union[LinkOption, List[LinkOption]] = None) -> Receiver:
        """Initiate a link to receive messages (subscription).

        Args:
            context: One of: created session, connection with or without
                established session, or url to create session.
            source: Source address.
            target: Target address.
            name: Name of the link.
            dynamic: Wether a dynamic AMQP queue should be generated.
            handler: Custom handler to handle received message.
            options: LinkOptions to further control the attachment.

        Returns:
            Receiver: A Qpid Receiver link over which messages are received.
        """
        receiver = super().create_receiver(context, source, target, name,
                                           dynamic, handler, options)
        self.touch()

        return receiver

    def create_sender(
            self, context: Union[Connection, Session, Url, str],
            target=None, source=None, name=None, handler=None, tags=None,
            options: Union[LinkOption, List[LinkOption]] = None) -> Sender:
        """Initiate a link to send messages.

        Args:
            context: One of: created session, connection with or without
                established session, or url to create session.
            target: Target address.
            source: Source address.
            name: Name of the link.
            handler: Custom handler to handle received message.
            tags:
            options: LinkOptions to further control the attachment.

        Returns:
            Sender: A Qpid Sender link over which messages are sent.
        """
        sender = super().create_sender(context, target, source, name, handler,
                                       tags, options)
        self.touch()

        return sender
