from datetime import timedelta
from functools import wraps
from random import shuffle
from time import sleep
from unittest import TestCase
from uuid import uuid4

import pytest
from proton import Message

from qpid_bow import Priority, ReconnectStrategy
from qpid_bow.config import configure
from qpid_bow.exc import TimeoutReached, UnroutableMessage
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.sender import Sender
from qpid_bow.receiver import Receiver

from . import TEST_AMQP_SERVER

CONFIG = {
    'amqp_url': TEST_AMQP_SERVER
}


class TestSender(TestCase):
    def setUp(self):
        configure(CONFIG)
        self.received_messages = []
        self.expected_messages = []

        queue_address = uuid4().hex
        create_queue(queue_address, durable=False,
                     auto_delete=True, priorities=10,
                     extra_properties={'qpid.auto_delete_timeout': 10})

        self.sender = Sender(queue_address)

        def handle_received_message(message: Message):
            self.received_messages.append(message)
            return True

        self.receiver = Receiver(handle_received_message, queue_address)

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
        self.expected_messages.append(message)
        self.sender.queue((message,))
        self.sender.send()

        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass

        self.assertEqual(len(self.received_messages),
                         len(self.expected_messages))
        for received, expected in zip(self.received_messages,
                                      self.expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)
            self.assertEqual(received.properties, expected.properties)

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
        self.sender.queue(messages_to_send)
        self.sender.send()

        # Should be received in order
        self.expected_messages.extend(
            sorted(messages_to_send, reverse=True,
                   key=lambda message: message.priority))

        # Give Qpid a bit of time for ordering if needed
        sleep(3)

        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass

        self.assertEqual(len(self.received_messages),
                         len(self.expected_messages))
        for received, expected in zip(self.received_messages,
                                      self.expected_messages):
            self.assertEqual(received.body, expected.body)

    def test_addressless_routable(self):
        message = create_message(b'FOOBAR')
        message.address = self.sender.address
        self.expected_messages.append(message)

        self.sender.address = None
        self.sender.queue((message,))
        self.sender.send()

        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass

        self.assertEqual(self.received_messages[0].id,
                         self.expected_messages[0].id)

    def test_addressless_unroutable(self):
        with self.assertRaises(UnroutableMessage):
            sender = Sender()
            message = create_message(b'FOOBAR')
            sender.queue((message,))
