"""Gather session-related data from AMQP broker."""

from collections import defaultdict
from datetime import timedelta
from typing import (
    MutableMapping,
    Optional,
)

from proton import Message

from qpid_bow.management import create_QMF2_query
from qpid_bow.remote_procedure import RemoteProcedure


def get_sessions(server_url: Optional[str] = None) -> dict:
    """Retrieve sessions from AMQP broker.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: A dict mapping between session id and it's address.

    Example:
        >>> get_sessions()
        {'org.apache.qpid.broker:session:0x7fb8bc021ab0': {'address': '10.0.0.2:34814'}}
    """
    sessions = {}

    def _update_sessions(message: Message):
        for item in message.body:
            values = item['_values']
            connectionRef = values['connectionRef']['_object_name'].decode()
            sessions[item['_object_id']['_object_name'].decode()] = {
                'address': connectionRef.rsplit('-', 1)[1]}
        return True

    rpc = RemoteProcedure(_update_sessions, 'qmf.default.direct',
                          server_url)
    binding_query_message = create_QMF2_query('org.apache.qpid.broker',
                                              'session')
    rpc.call(binding_query_message, timedelta(seconds=5))
    return sessions


def get_outgoing_sessions_by_address(
        server_url: Optional[str] = None) -> MutableMapping[str, list]:
    """Retrieve outgoing sessions from AMQP broker.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        defaultdict: A dict mapping between address name and list of sessions.

    Example:
        >>> get_outgoing_sessions_by_address()
        {'8152f68b-c74a-4d22-8630-a89cf194d067_8152f68b-\
c74a-4d22-8630-a89cf194d067-2d808664-fe81-4da4-8258-288a7ff531ac': [{\
'session_id': 'org.apache.qpid.broker:session:0x7fb8bc021ab0','transfers': \
ulong(0)}]}
    """
    client_subscriptions: MutableMapping[str, list] = defaultdict(list)

    def _update_results(message: Message):
        for value in (item['_values'] for item in message.body):
            client_subscriptions[value['source'].decode()].append({
                'session_id': value['sessionRef']['_object_name'].decode(),
                'transfers': value['transfers']
            })
        return True

    rpc = RemoteProcedure(_update_results, 'qmf.default.direct',
                          server_url)
    binding_query_message = create_QMF2_query('org.apache.qpid.broker',
                                              'outgoing')
    rpc.call(binding_query_message, timedelta(seconds=5))
    return dict(client_subscriptions)
