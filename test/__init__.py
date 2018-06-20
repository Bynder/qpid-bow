from contextlib import suppress
from datetime import timedelta
from os import environ
from typing import Iterable, Optional
from unittest import TestCase
from uuid import uuid4

from proton import Message

from qpid_bow.config import configure
from qpid_bow.exc import TimeoutReached
from qpid_bow.management.queue import create_queue
from qpid_bow.sender import Sender
from qpid_bow.receiver import Receiver

TEST_AMQP_SERVER = environ.get('AMQP_TEST_SERVERS', 'amqp://127.0.0.1')
CONFIG = {
    'amqp_url': TEST_AMQP_SERVER
}


class MessagingTestBase(TestCase):
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

    def send_messages(self, messages: Iterable[Message]):
        self.expected_messages.extend(messages)
        self.sender.queue(messages)
        self.sender.send()

    def receive_messages(self):
        with suppress(TimeoutReached):
            self.receiver.receive(timeout=timedelta(seconds=2))

    def check_messages(self, check_identity: bool = True,
                       expected_messages: Optional[Iterable[Message]] = None):
        expected_messages = expected_messages or self.expected_messages
        self.assertEqual(len(self.received_messages), len(expected_messages))
        for received, expected in zip(self.received_messages,
                                      expected_messages):
            self.assertEqual(received.body, expected.body)
            if check_identity:
                # Check identity 100% 
                self.assertEqual(received.id, expected.id)
                self.assertEqual(received.properties, expected.properties)
