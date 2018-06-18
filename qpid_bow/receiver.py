"""Receive messages from AMQP broker."""

import asyncio
import logging
from datetime import timedelta, datetime
from inspect import signature
from typing import Any, Callable, Coroutine, Optional, Type, Union

from proton import Message, Delivery
from proton.reactor import (
    Container,
    EventBase,
    Task,
)

from qpid_bow import Connector, RunState
from qpid_bow.exc import (
    QMF2Exception,
    RetriableMessage,
    TimeoutReached,
)

logger = logging.getLogger()


ReceiveCallback = Union[
    Callable[[Message], bool],
    Callable[[Message, Delivery], bool],
    Callable[[Message], Coroutine[Any, Any, bool]],
]


class Receiver(Connector):
    """Callback based AMQP message receiver.

    Args:
        callback: Function to call when new message is received.
        address: Name of queue or exchange from where to receive the messages.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
        limit: Limit the amount of messages to receive.
        container_class: Qpid Proton reactor container-class to use.
    """
    # pylint: disable=too-many-arguments
    def __init__(self, callback: ReceiveCallback,
                 address: Optional[str] = None,
                 server_url: Optional[str] = None,
                 limit: Optional[int] = None,
                 container_class: Type[Any] = Container) -> None:
        super().__init__(server_url=server_url,
                         container_class=container_class)
        self.limit = limit
        self.received = 0
        self.timeout: Optional[timedelta] = None
        self.timeout_task: Task = None
        self.receivers: dict = {}
        self.connection = None

        self.callback = callback
        self.advanced_callback = len(signature(callback).parameters) == 2
        self.timeout_reached: bool
        self.start_time: Optional[datetime]
        self.timeout_reached = False

        if address:
            self.receivers[address] = None

    def receive(self, timeout: Optional[timedelta] = None):
        """Start receive loop for up to timeout duration or limit messages.

        Args:
            timeout: Timeout duration to wait for message.
        """
        self.timeout_reached = False
        self.timeout = timeout

        # Give control to container to do our receiving
        self.run()

        if self.timeout_reached:
            raise TimeoutReached()

    def add_address(self, address: str):
        """Start receiving messages from the given additional address.

        Args:
            address: Queue or exchange address to receive from.
        """
        if address in self.receivers:
            return

        if self.connection:
            self._start_receiver(address)
        else:
            self.receivers[address] = None

    def remove_address(self, address: str):
        """Stop receiving messages from the given address.

        Args:
            address: Queue or exchange address to stop receiving from.
        """
        receiver = self.receivers.pop(address)
        receiver.close()

    def _start_receiver(self, address: str):
        if address == '#':  # AMQP-dynamic queue address
            receiver = self.container.create_receiver(self.connection,
                                                      dynamic=True)
        else:
            receiver = self.container.create_receiver(self.connection,
                                                      address)
        self.receivers[address] = receiver

    def _restart_receivers(self):
        addresses = list(self.receivers.keys())
        self.receivers.clear()
        for address in addresses:
            self._start_receiver(address)

    def stop(self):
        super().stop()
        if self.timeout_task:
            self.timeout_task.cancel()
            self.timeout_task = None

        self.received = 0
        self.start_time = None
        self.receivers.clear()
        self.container = None

    def on_transport_error(self, event: EventBase):
        super().on_transport_error(event)
        logger.warning("AMQP transport was closed, reconnecting...")
        self._restart_receivers()

    def on_timer_task(self, event: EventBase):
        """Handles the event when a timer is finished.

        Args:
            event: Reactor timer task event object.
        """
        if self.run_state != RunState.started:
            return

        if (datetime.utcnow() - self.start_time) > self.timeout:
            self.timeout_reached = True
            self.stop()
        else:
            self.timeout_task = event.container.schedule(0.25, self)

    def on_start(self, event):
        super().on_start(event)
        if self.run_state == RunState.started:
            self.start_time = datetime.utcnow()
            if self.timeout:
                self.timeout_task = event.container.schedule(0.25, self)
            self._restart_receivers()

    def on_message(self, event):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # There is no event loop in current thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        handle_message_task = loop.create_task(self._handle_message(event))
        if not loop.is_running():
            loop.run_until_complete(handle_message_task)

    async def _handle_message(self, event):
        if not self.start_time:
            # Eagerly got more messages then we're interested in, release
            self.release(event.delivery)
            return

        try:
            if self.advanced_callback:
                success = self.callback(event.message, event.delivery)
            else:
                success = self.callback(event.message)
            if asyncio.iscoroutine(success):
                success = await success
            if success:
                self.accept(event.delivery)
            else:
                self.reject(event.delivery)
        except RetriableMessage:
            if event.message.delivery_count:
                self.reject(event.delivery)
            else:
                # Set the delivery status before releasing back into the queue
                # https://bugzilla.redhat.com/show_bug.cgi?id=1283652
                event.delivery.local.undeliverable = True
                event.delivery.local.failed = True
                self.release(event.delivery)
        except QMF2Exception:
            self.accept(event.delivery)
            raise
        except Exception:
            # On unexpected error, we will reject the message and re-raise
            logger.error('Unexpected error, rejecting the message',
                         exc_info=True)
            self.reject(event.delivery)
            raise
        finally:
            self.touch()
            self.received += 1
            if self.received == self.limit:
                self.stop()
