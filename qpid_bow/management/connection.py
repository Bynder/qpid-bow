"""AMQP broker connection management."""

from datetime import timedelta
from typing import Optional

from proton import Message

from qpid_bow.management import (
    create_QMF2_method_invoke,
    create_QMF2_query,
    handle_QMF2_exception,
)
from qpid_bow.remote_procedure import RemoteProcedure


def get_connection_ids(server_url: Optional[str] = None) -> list:
    """Retrieve connection ids of all established connections to AMQP broker.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        List of connections
    """
    connections = []

    def _update_connections(message: Message):
        for item in message.body:
            connections.append(item['_object_id']['_object_name'])
        return True

    rpc = RemoteProcedure(_update_connections, 'qmf.default.direct',
                          server_url)
    binding_query_message = create_QMF2_query('org.apache.qpid.broker',
                                              'connection')
    rpc.call(binding_query_message, timedelta(seconds=5))
    return connections


def kill_connection(connection_id: dict, server_url: Optional[str] = None):
    """Kill connection on AMQP broker.

    Args:
        connection_id: ID of connection.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    rpc.call(create_QMF2_method_invoke(connection_id, 'close', {}),
             timedelta(seconds=5))
