from datetime import timedelta
from uuid import uuid4

from qpid_bow.exc import TimeoutReached
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.sender import Sender

from . import MessagingTestBase


class TestReceiver(MessagingTestBase):
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
        expected_messages = [create_message(b'FOOBAR1'),
                             create_message(b'FOOBAR2'),
                             create_message(b'FOOBAR3')]
        self.send_messages(expected_messages[0:-1])

        second_queue_address = uuid4().hex
        create_queue(second_queue_address, durable=False, auto_delete=True,
                     extra_properties={'qpid.auto_delete_timeout': 10})
        self.receiver.add_address(second_queue_address)

        second_sender = Sender(second_queue_address)
        second_sender.queue(expected_messages[-1:])
        second_sender.send()

        self.receive_messages()
        self.assertEqual(len(self.received_messages), len(expected_messages))

    def test_receive(self):
        self.send_messages((create_message(b'FOOBAR1'),
                            create_message(b'FOOBAR2'),
                            create_message(b'FOOBAR3')))
        self.receive_messages()
        self.check_messages()

    def test_receive_limit(self):
        self.send_messages((create_message(b'FOOBAR1'),
                            create_message(b'FOOBAR2'),
                            create_message(b'FOOBAR3')))

        # Should first receive only 2
        self.receiver.limit = 2
        self.receive_messages()
        self.check_messages(expected_messages=self.expected_messages[:-1])

        # Receive the rest
        self.receiver.limit = None
        self.receive_messages()
        self.check_messages()
