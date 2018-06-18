"""Remote procedure call handling."""

from datetime import timedelta
from logging import getLogger
from typing import Optional
from warnings import warn

from proton import Message

from qpid_bow import ReconnectStrategy, RunState
from qpid_bow.receiver import (
    Receiver,
    ReceiveCallback,
)

logger = getLogger()


class RemoteProcedure(Receiver):
    """This class can be used to handle a simple RPC pattern,
       sending a call message and waiting for a reply on a temporary queue
       and response handling through callbacks.

    Args:
        callback: Function to call when new message is received.
        address: Address of queue or exchange to send the messages to.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
        reconnect_strategy: Strategy to use on connection drop.
    """
    def __init__(
            self, callback: ReceiveCallback,
            address: str, server_url: Optional[str] = None,
            reconnect_strategy: ReconnectStrategy = ReconnectStrategy.failover
    ) -> None:
        super().__init__(callback, '#', server_url,
                         reconnect_strategy=reconnect_strategy)
        if self.reconnect_strategy == ReconnectStrategy.backoff:
            warn("Using ReconnectStrategy.backoff may cause Sender to block")
        self.send_address = address
        self.message: Message

    def call(self, message: Message, timeout: Optional[timedelta] = None):
        """Send RPC message and wait for call reply.
        Args:
            message: Message to send to RPC-address.
            timeout: Optional maximum timeout to wait for a reply.
        """
        self.message = message

        # Receiver.receive() triggers container run with timeout
        self.receive(timeout)

    def on_start(self, event):
        super().on_start(event)
        if self.run_state == RunState.started:
            event.container.create_sender(self.connection, self.send_address)

    def on_sendable(self, event):
        if not self.reply_to:
            # Wait until receiver has been set up
            return

        self.message.reply_to = self.reply_to
        event.sender.send(self.message)

        # We only need to send one message
        event.sender.close()

    def on_message(self, event):
        super().on_message(event)
        if 'partial' not in event.message.properties:
            self.stop()


    @property
    def reply_to(self):
        """Reply to address of our temporary queue."""
        return self.receivers['#'].remote_source.address
