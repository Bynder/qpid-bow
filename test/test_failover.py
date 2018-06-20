from datetime import timedelta
from unittest import TestCase
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
                     auto_delete=True, priorities=5,
                     extra_properties={'qpid.auto_delete_timeout': 10})
        self.sender = Sender(queue_address)

        def handle_received_message(message: Message):
            self.received_messages.append(message)
            return True
        self.receiver = Receiver(handle_received_message, queue_address)

    def test_failover_receive(self):
        expected_messages = (create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'),
                             create_message(b'FOOBAR'))
        self.sender.queue(expected_messages)
        self.sender.send()

        self.receiver.limit = 3
        self.receiver.receive(timeout=timedelta(seconds=2))
        for received, expected in zip(self.received_messages,
                                      expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)
