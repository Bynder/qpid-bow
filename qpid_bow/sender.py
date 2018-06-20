"""Send messages to AMPQ broker."""

import logging
from typing import Iterable, Optional
from uuid import uuid4
from warnings import warn

from proton import Message

from qpid_bow import Connector, ReconnectStrategy, RunState
from qpid_bow.exc import UnroutableMessage

logger = logging.getLogger()


class Sender(Connector):
    """Class to send messages in a batch to an AMQP address.

    Args:
        address: Address of queue or exchange to send the messages to.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
        reconnect_strategy: Strategy to use on connection drop.
    """

    def __init__(
            self, address: Optional[str] = None,
            server_url: Optional[str] = None,
            reconnect_strategy: ReconnectStrategy = ReconnectStrategy.failover
    ) -> None:
        super().__init__(server_url, reconnect_strategy=reconnect_strategy)
        if self.reconnect_strategy == ReconnectStrategy.backoff:
            warn("Using ReconnectStrategy.backoff may cause Sender to block")
        self.address = address
        self.send_queue: list = []

    def queue(self, messages: Iterable[Message]):
        """Enqueue messages that will be send on calling :obj:`send`."""
        if not self.address:
            if any((new_message.address is None for new_message in messages)):
                raise UnroutableMessage(
                    "A Sender with no address requires Message.address is set")

        self.send_queue.extend(messages)

    def send(self):
        """Send queued messages."""
        # Give control to container to do our sending
        if self.send_queue:
            self.run()

    def on_start(self, event):
        super().on_start(event)
        if self.run_state == RunState.started:
            event.container.create_sender(self.connection, self.address)

    def on_sendable(self, event):
        """Handles sendable event, sends all the messages in the send_queue."""
        if not self.connection:
            return

        message = self.send_queue.pop(0)
        message.id = uuid4()
        # TODO SHA
        event.sender.send(message)

        if not self.send_queue:
            # We are done sending, clear & return control
            self.stop()
