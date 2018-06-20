from random import shuffle
from time import sleep
from uuid import uuid4

import pytest

from qpid_bow import Priority, ReconnectStrategy
from qpid_bow.exc import UnroutableMessage
from qpid_bow.message import create_message
from qpid_bow.sender import Sender

from . import MessagingTestBase


class TestSender(MessagingTestBase):
    def test_reconnect_strategy_backoff_warning(self):
        with pytest.warns(UserWarning):
            Sender(uuid4().hex, reconnect_strategy=ReconnectStrategy.backoff)

    def test_connection_error(self):
        sender = Sender(
            uuid4().hex, server_url='amqp://invalid:invalid@example')
        sender.queue((create_message(b'FOOBAR'),))
        with self.assertRaises(ConnectionError):
            sender.send()

    def test_send_properties(self):
        message = create_message(b'FOOBAR', {'foo': 'bar', 'baz': 123})
        self.send_messages((message,))
        self.receive_messages()
        self.check_messages()

    def test_send_priorities(self):
        messages_to_send = []
        for i in range(0, 30):
            messages_to_send.append(
                create_message(b'LOW', priority=Priority.low))
            messages_to_send.append(
                create_message(b'NORMAL', priority=Priority.normal))
            messages_to_send.append(
                create_message(b'HIGH', priority=Priority.high))

        # Send messages with priority in random order
        shuffle(messages_to_send)
        self.send_messages(messages_to_send)

        # Should be received in order
        self.expected_messages = sorted(messages_to_send, reverse=True,
                                        key=lambda message: message.priority)

        # Give Qpid a bit of time for ordering if needed
        sleep(3)
        self.receive_messages()

        # Don't check identity, because only the priority order is preserved
        self.check_messages(check_identity=False)

    def test_addressless_routable(self):
        message = create_message(b'FOOBAR')
        message.address = self.sender.address
        self.sender.address = None
        self.send_messages((message,))
        self.receive_messages()
        self.check_messages()

    def test_addressless_unroutable(self):
        with self.assertRaises(UnroutableMessage):
            sender = Sender()
            message = create_message(b'FOOBAR')
            sender.queue((message,))
