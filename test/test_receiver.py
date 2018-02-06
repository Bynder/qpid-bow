from datetime import timedelta
from functools import partial
from unittest import TestCase
from uuid import uuid4

from proton import Message

from qpid_bow.config import configure
from qpid_bow.exc import TimeoutReached
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.receiver import Receiver
from qpid_bow.sender import Sender

CONFIG = {
    'amqp_url': 'amqp://127.0.0.1'
}


class TestReceiver(TestCase):
    def setUp(self):
        configure(CONFIG)
        self.received_messages = []

        queue_address = uuid4().hex
        create_queue(queue_address, durable=False,
                     auto_delete=True, priorities=5)
        self.sender = Sender(queue_address)
        self.receiver = Receiver(
            partial(TestReceiver.handle_received_message, self), queue_address)

    def handle_received_message(self, message: Message):
        self.received_messages.append(message)
        return True

    def test_receive_timeout(self):
        with self.assertRaises(TimeoutReached):
            self.receiver.receive(timeout=timedelta(seconds=2))
        self.assertEqual(len(self.received_messages), 0)

    def test_multi_receive(self):
        expected_messages = [create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'),
                             create_message(b'FOOBAR')]
        self.sender.queue(expected_messages[0:-1])
        self.sender.send()

        second_queue_address = uuid4().hex
        create_queue(second_queue_address, durable=False, auto_delete=True)
        self.receiver.add_address(second_queue_address)

        second_sender = Sender(second_queue_address)
        second_sender.queue(expected_messages[-1:])
        second_sender.send()

        self.receiver.receive(timeout=timedelta(seconds=2))
        for received, expected in zip(self.received_messages,
                                      expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)

    def test_receive(self):
        expected_messages = (create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'))
        self.sender.queue(expected_messages)
        self.sender.send()

        self.receiver.receive(timeout=timedelta(seconds=2))
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
        self.receiver.receive(timeout=timedelta(seconds=2))
        for received, expected in zip(self.received_messages,
                                      expected_messages[:-1]):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)

        # Receive remaining message
        self.receiver.receive(timeout=timedelta(seconds=2))
        for received, expected in zip(self.received_messages,
                                      expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)
