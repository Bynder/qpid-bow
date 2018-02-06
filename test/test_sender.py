from functools import wraps
from random import shuffle
from time import sleep
from unittest import TestCase
from uuid import uuid4

from proton.handlers import MessagingHandler
from proton.reactor import Container

from qpid_bow import Priority
from qpid_bow.config import configure
from qpid_bow.exc import UnroutableMessage
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.sender import Sender


CONFIG = {
    'amqp_url': 'amqp://127.0.0.1'
}


def messaging_test(f):
    @wraps(f)
    def wrapper(self):
        self.active_test = f
        Container(self).run()
        for received, expected in zip(self.received_messages,
                                      self.expected_messages):
            self.assertEqual(received.id, expected.id)
            self.assertEqual(received.body, expected.body)
            self.assertEqual(received.properties, expected.properties)

    return wrapper


class TestSender(TestCase, MessagingHandler):
    def __init__(self, methodName):
        TestCase.__init__(self, methodName)
        MessagingHandler.__init__(self)

    def setUp(self):
        configure(CONFIG)
        self.received_messages = []
        self.expected_messages = []

    def on_start(self, event):
        super().on_start(event)

        queue_address = uuid4().hex

        # Setup a dynamic receiver for receiving test results
        create_queue(queue_address, durable=False,
                     auto_delete=True, priorities=5)
        conn = event.container.connect(CONFIG['amqp_url'])
        self.receiver = event.container.create_receiver(conn, queue_address)

        # Setup sender for tests with dynamic address
        self.sender = Sender(queue_address)

        # Run test
        self.active_test(self)

    def on_message(self, event):
        self.received_messages.append(event.message)
        self.accept(event.delivery)
        if len(self.received_messages) == len(self.expected_messages):
            event.receiver.close()
            event.connection.close()

    @messaging_test
    def test_send_single(self):
        message = create_message(b'FOOBAR')
        self.expected_messages.append(message)

        self.sender.queue((message,))
        self.sender.send()

    @messaging_test
    def test_send_multiple(self):
        messages = (create_message(b'FOOBAR'), create_message(b'FOOBAR2'))
        self.expected_messages.extend(messages)
        self.sender.queue(messages)
        self.sender.send()

    @messaging_test
    def test_send_properties(self):
        message = create_message(b'FOOBAR', {'foo': 'bar', 'baz': 123})
        self.expected_messages.append(message)
        self.sender.queue((message,))
        self.sender.send()

    @messaging_test
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
            sorted(messages_to_send, key=lambda message: message.priority))

        # Give Qpid a bit of time for ordering if needed
        sleep(3)

    @messaging_test
    def test_addressless_routable(self):
        message = create_message(b'FOOBAR')
        message.address = self.sender.address
        self.expected_messages.append(message)

        self.sender.address = None
        self.sender.queue((message,))
        self.sender.send()

    def test_addressless_unroutable(self):
        with self.assertRaises(UnroutableMessage):
            sender = Sender()
            message = create_message(b'FOOBAR')
            sender.queue((message,))
