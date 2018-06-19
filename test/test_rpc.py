from unittest import TestCase
from uuid import uuid4

import pytest

from qpid_bow import ReconnectStrategy
from qpid_bow.config import configure
from qpid_bow.message import create_message
from qpid_bow.remote_procedure import RemoteProcedure

from . import TEST_AMQP_SERVER

CONFIG = {
    'amqp_url': TEST_AMQP_SERVER
}


class TestRemoteProcedure(TestCase):
    def setUp(self):
        configure(CONFIG)

    def test_reconnect_strategy_backoff_warning(self):
        with pytest.warns(UserWarning):
            RemoteProcedure(lambda m: True, uuid4().hex,
                            reconnect_strategy=ReconnectStrategy.backoff)

    def test_connection_error(self):
        rpc = RemoteProcedure(lambda m: True, uuid4().hex,
                              server_url='amqp://invalid:invalid@example')
        with self.assertRaises(ConnectionError):
            rpc.call(create_message(b'FOOBAR'))
