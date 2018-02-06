from unittest import TestCase
from uuid import uuid4

from proton import symbol

from qpid_bow import Priority
from qpid_bow.exc import UnroutableMessage
from qpid_bow.message import create_message, create_reply


class TestMesageCreate(TestCase):
    def test_content_type_none_native_list(self):
        message = create_message([1, 2, 3])
        self.assertEqual(message.content_type, symbol('None'))

    def test_content_type_none_native_dict(self):
        message = create_message({'foo': 'bar'})
        self.assertEqual(message.content_type, symbol('None'))


class TestMesageCreateDefault(TestCase):
    def setUp(self):
        self.properties = {'foo': 'bar'}
        self.message = create_message(b'foobar', self.properties)

    def test_default_priority(self):
        self.assertEqual(Priority(self.message.priority),
                         Priority.normal)

    def test_properties(self):
        self.assertEqual(self.message.properties, self.properties)

    def test_content_type(self):
        self.assertEqual(self.message.content_type, 'application/octet-stream')

    def test_durable(self):
        self.assertTrue(self.message.durable)


class TestMessageCreateReply(TestCase):
    def setUp(self):
        self.properties = {'foo': 'bar'}
        self.origin_message = create_message(
            b'foobar', self.properties, Priority.high)
        self.origin_message.reply_to = 'foobar_address'
        self.origin_message.correlation_id = uuid4()

        self.reply_message = create_reply(self.origin_message, b'foobar-reply')

    def test_unroutable(self):
        with self.assertRaises(UnroutableMessage):
            create_reply(create_message(b'foobar'), b'foobar')

    def test_properties(self):
        expected_properties = {'is_reply': True}
        expected_properties.update(self.origin_message.properties)
        self.assertEqual(self.reply_message.properties, expected_properties)

    def test_content_type(self):
        self.assertEqual(self.reply_message.content_type,
                         'application/octet-stream')

    def test_durable(self):
        self.assertTrue(self.reply_message.durable)

    def test_correlation_id(self):
        self.assertEqual(self.reply_message.correlation_id,
                         self.origin_message.correlation_id)

    def test_address(self):
        self.assertEqual(self.reply_message.address,
                         self.origin_message.reply_to)

    def test_priority(self):
        self.assertEqual(self.reply_message.priority,
                         self.origin_message.priority)
