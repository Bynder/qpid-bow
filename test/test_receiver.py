from datetime import timedelta
from unittest import TestCase
from uuid import uuid4

from proton import Message

from qpid_bow.config import configure
from qpid_bow.exc import TimeoutReached
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.receiver import Receiver
from qpid_bow.sender import Sender

from . import TEST_AMQP_SERVER

CONFIG = {
    'amqp_url': TEST_AMQP_SERVER
}


class TestReceiver(TestCase):
    def setUp(self):
        configure(CONFIG)
        self.received_messages = []

        queue_address = uuid4().hex
        create_queue(queue_address, durable=False,
                     auto_delete=True, priorities=5,
                     extra_properties={'qpid.auto_delete_timeout': 10})
        self.sender = Sender(queue_address)

        def handle_received_message(message: Message):
            self.received_messages.append(message)
            return True

        self.receiver = Receiver(handle_received_message, queue_address)

    def test_receive_timeout(self):
        with self.assertRaises(TimeoutReached):
            self.receiver.receive(timeout=timedelta(seconds=2))
        self.assertEqual(len(self.received_messages), 0)

    def test_add_address_duplicate(self):
        second_queue_address = uuid4().hex
        self.receiver.add_address(second_queue_address)
        self.receiver.add_address(second_queue_address)
        self.assertEqual(len(self.receiver.receivers), 2)

    def test_multi_receive(self):
        expected_messages = [create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'),
                             create_message(b'FOOBAR')]
        self.sender.queue(expected_messages[0:-1])
        self.sender.send()

        second_queue_address = uuid4().hex
        create_queue(second_queue_address, durable=False, auto_delete=True,
                     extra_properties={'qpid.auto_delete_timeout': 10})
        self.receiver.add_address(second_queue_address)

        second_sender = Sender(second_queue_address)
        second_sender.queue(expected_messages[-1:])
        second_sender.send()

        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass
        self.assertEqual(len(self.received_messages), len(expected_messages))

    def test_receive(self):
        expected_messages = (create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'))
        self.sender.queue(expected_messages)
        self.sender.send()

        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass
        self.assertEqual(len(self.received_messages), len(expected_messages))
        for received, expected in zip(self.received_messages,
                                      expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)

    def test_receive_limit(self):
        expected_messages = (create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'))
        self.sender.queue(expected_messages)
        self.sender.send()

        # Should first receive only 2
        self.receiver.limit = 2
        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass
        self.assertEqual(len(self.received_messages), 2)
        for received, expected in zip(self.received_messages,
                                      expected_messages[:-1]):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)

        # Receive remaining message
        try:
            self.receiver.receive(timeout=timedelta(seconds=2))
        except TimeoutReached:
            pass
        for received, expected in zip(self.received_messages,
                                      expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)
