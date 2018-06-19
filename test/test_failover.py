from datetime import timedelta
from functools import partial
from unittest import TestCase, skip
from uuid import uuid4

from proton import Message

from qpid_bow.config import configure
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.receiver import Receiver
from qpid_bow.sender import Sender

from . import TEST_AMQP_SERVER

CONFIG = {
    'amqp_url': f'amqp://127.0.0.1:5432, {TEST_AMQP_SERVER}'
}


class TestFailoverReceiver(TestCase):
    def setUp(self):
        configure(CONFIG)
        self.received_messages = []

        queue_address = uuid4().hex
        create_queue(queue_address, durable=False,
                     auto_delete=True, priorities=5)
        self.sender = Sender(queue_address)
        self.receiver = Receiver(
            partial(TestFailoverReceiver.handle_received_message, self),
            queue_address)

    @skip
    def handle_received_message(self, message: Message):
        self.received_messages.append(message)
        return True

    def test_failover_receive(self):
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
