"""Qpid-bow client framework."""

from asyncio import Event as AsyncioEvent
from enum import Enum, auto
from logging import getLogger
from typing import Optional, Type

from proton import Connection
from proton.handlers import MessagingHandler
from proton.reactor import Container, Backoff, EventBase

from qpid_bow.config import (
    config,
    get_urls,
)

logger = getLogger()


class Priority(Enum):
    """Convenience enum for message priorities.

    Qpid supports a configurable amount of priorities for a queue,
    be sure to have at least 5.

    When used on a message and enabled on a queue Qpid will re-order which
    get send out to a receiver first based on the priority.
    """
    internal_low = 0
    low = 1
    normal = 2
    high = 3
    realtime = 4


class RunState(Enum):
    """Indicate current state of Connector."""
    stopped = auto()
    stopping = auto()
    started = auto()
    reconnecting = auto()
    connected = auto()
    failed = auto()


class NonBackoff(Backoff):
    def next(self):
        return self.delay


class ReconnectStrategy(Enum):
    """Define possible reconnect strategies."""
    backoff = Backoff()
    failover = NonBackoff()
    disabled = False


class Connector(MessagingHandler):
    """Initiate and keep connection to AMQP message broker.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
        container_class: Qpid Proton reactor container-class to use.
        reconnect_strategy: Strategy to use on connection drop.
    """
    def __init__(
            self, server_url: Optional[str] = None,
            container_class: Type[Container] = Container,
            reconnect_strategy: ReconnectStrategy = ReconnectStrategy.backoff
        ) -> None:
        super().__init__(auto_accept=False)
        self.server_urls = get_urls(server_url)

        self.run_state = RunState.stopped
        self.failover_count = 0
        self.container_class = container_class
        self.reconnect_strategy = reconnect_strategy
        self.close_event = AsyncioEvent()
        self.close_event.set()
        self.connection: Connection
        self.container: Container

    def touch(self):
        """Instruct the reactor container to do processing.

        When running with an alternative container, like the AsyncioContainer,
        you might need to call this to startup new sessions.
        """
        try:
            self.container.touch()
        except (AttributeError, NameError):
            pass

    def on_start(self, event: EventBase):
        """Handle start event.

        Args:
            event: Reactor init event object with container to connect to.
        """
        super().on_start(event)
        if self.run_state == RunState.stopped:
            logger.debug("Starting %s", self)
            self.run_state = RunState.started
            self.connection = event.container.connect(
                urls=self.server_urls, reconnect=self.reconnect_strategy.value)

    def on_connection_opened(self, event: EventBase):  # pylint: disable=unused-argument
        self.run_state = RunState.connected
        self.close_event.clear()

    def on_transport_error(self, event: EventBase):
        super().on_transport_error(event)
        if self.reconnect_strategy == ReconnectStrategy.backoff:
            logger.warning("AMQP transport was closed, reconnecting...")
            self.run_state = RunState.reconnecting
            return
        elif self.reconnect_strategy == ReconnectStrategy.failover:
            self.failover_count += 1
            if (len(self.server_urls) * 2) > self.failover_count:
                return

        self.run_state = RunState.failed
        condition = event.transport.condition
        raise ConnectionError(f"{condition.name} {condition.description}")

    def on_connection_closed(self, event: EventBase):
        """Handle close connection event.

        Args:
            event: Connection close event.
        """
        logger.debug("Connection %s confirmed closed", self)
        self.run_state = RunState.stopped
        self.close_event.set()

    async def wait_closed(self):
        await self.close_event.wait()

    def run(self):
        """Start this Connector and setup connection to the AMQP server."""
        self.container = self.container_class(self)
        self.container.run()

    def stop(self):
        """Stop connection to the AMQP server."""
        if self.run_state not in (RunState.started,
                                  RunState.connected,
                                  RunState.reconnecting):
            return

        logger.debug("Connection %s closing", self)
        self.run_state = RunState.stopping
        self.connection.close()
        self.connection = None

        # Give control to container to cleanup
        try:
            self.container.touch()
        except (AttributeError, NameError):
            self.container.process()
