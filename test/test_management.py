from unittest import TestCase
from uuid import uuid4

from qpid_bow.config import configure
from qpid_bow.exc import ObjectNotFound, QMF2NotFound, QMF2ObjectExists
from qpid_bow.management.exchange import (
    create_binding,
    create_exchange,
    delete_exchange,
    ExchangeType
)
from qpid_bow.management.queue import (
    create_queue,
    delete_queue,
)

from . import TEST_AMQP_SERVER

CONFIG = {
    'amqp_url': TEST_AMQP_SERVER
}


class TestManagement(TestCase):
    def setUp(self):
        configure(CONFIG)
        self.queue_address = uuid4().hex
        self.exchange_address = uuid4().hex

    def tearDown(self):
        try:
            delete_queue(self.queue_address)
        except QMF2NotFound:
            pass
        try:
            delete_exchange(self.exchange_address)
        except QMF2NotFound:
            pass

    def test_create_queue(self):
        create_queue(self.queue_address, durable=False, auto_delete=True)

    def test_create_queue_already_exists(self):
        create_queue(self.queue_address, durable=False, auto_delete=True)
        with self.assertRaises(QMF2ObjectExists):
            create_queue(self.queue_address, durable=False, auto_delete=True)

    def test_create_exchange(self):
        create_exchange(self.exchange_address,
                        exchange_type=ExchangeType.direct,
                        durable=False)

    def test_create_exchange_already_exists(self):
        create_exchange(self.exchange_address,
                        exchange_type=ExchangeType.direct,
                        durable=False)
        with self.assertRaises(QMF2ObjectExists):
            create_exchange(self.exchange_address,
                            exchange_type=ExchangeType.direct,
                            durable=False)

    def test_create_binding(self):
        create_exchange(self.exchange_address,
                        exchange_type=ExchangeType.topic,
                        durable=False)
        create_queue(self.queue_address, durable=False, auto_delete=True)
        create_binding(self.exchange_address, self.queue_address,
                       binding_name='one_ring')

    def test_create_headers_binding(self):
        create_exchange(self.exchange_address,
                        exchange_type=ExchangeType.headers,
                        durable=False)
        create_queue(self.queue_address, durable=False, auto_delete=True)
        create_binding(self.exchange_address,
                       self.queue_address,
                       headers_match={'type': 'food', 'timing': 'early'},
                       binding_name='breakfast')

    def test_create_binding_match_on_wrong_exchange(self):
        create_exchange(self.exchange_address,
                        exchange_type=ExchangeType.direct,
                        durable=False)
        create_queue(self.queue_address, durable=False, auto_delete=True)
        with self.assertRaises(RuntimeError):
            create_binding(self.exchange_address,
                           self.queue_address,
                           headers_match={'type': 'food', 'timing': 'late'},
                           binding_name='dinner')

    def test_create_binding_no_exchange(self):
        create_queue(self.queue_address, durable=False, auto_delete=True)
        with self.assertRaises(ObjectNotFound):
            create_binding(self.exchange_address, self.queue_address)

    def test_create_binding_no_queue(self):
        create_exchange(self.exchange_address,
                        exchange_type=ExchangeType.direct,
                        durable=False)
        with self.assertRaises(QMF2NotFound):
            create_binding(self.exchange_address, self.queue_address)
