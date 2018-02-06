"""AMQP broker queue management."""
from datetime import timedelta
from typing import (
    Optional,
    Tuple,
)

from qpid_bow.management import (
    create_QMF2_method_invoke,
    get_broker_id,
    get_object,
    handle_QMF2_exception,
)
from qpid_bow.remote_procedure import RemoteProcedure


def reroute_queue(queue_name: str, exchange_name: str,
                  limit: int = 0,
                  message_filter: Optional[Tuple[str, str]] = None,
                  server_url: Optional[str] = None):
    """Reroute messages from a queue to an exchange.

    Args:
        queue_name: Name of queue.
        exchange_name: Name of exchange.
        limit: Limit the amount of messages to reroute.
        message_filter: Filter based on property=value.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    # Gather queue & exchange info + existance check
    queue = get_object('org.apache.qpid.broker', 'queue', queue_name,
                       server_url)
    get_object('org.apache.qpid.broker', 'exchange',
               exchange_name, server_url)

    method_arguments = {'request': limit}  # type: dict

    if exchange_name:
        method_arguments['useAltExchange'] = False
        method_arguments['exchange'] = exchange_name
    else:
        method_arguments['useAltExchange'] = True
        method_arguments['exchange'] = ''

    if message_filter:
        method_arguments['filter'] = _build_message_filter(*message_filter)

    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    rpc.call(create_QMF2_method_invoke(queue['_object_id'],
                                       'reroute', method_arguments),
             timedelta(seconds=5))


def purge_queue(queue_name: str,
                limit: int = 0,
                message_filter: Optional[Tuple[str, str]] = None,
                server_url: Optional[str] = None):
    """ Purge a queue on the AMQP broker.

    Args:
        queue_name: Name of queue.
        limit: Limit the amount of messages to purge.
        message_filter: Filter based on property=value.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    queue = get_object('org.apache.qpid.broker', 'queue', queue_name,
                       server_url)
    method_arguments = {'request': limit}  # type: dict
    if message_filter:
        method_arguments['filter'] = _build_message_filter(*message_filter)

    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    rpc.call(create_QMF2_method_invoke(queue['_object_id'],
                                       'purge', method_arguments),
             timedelta(seconds=5))


def create_queue(queue_name: str,
                 durable: bool = True,
                 auto_delete: bool = False,
                 priorities: int = 0,
                 extra_properties: Optional[dict] = None,
                 server_url: Optional[str] = None):
    """Create a queue on the AMQP broker.

    Args:
        queue_name: Name of queue.
        durable: Persist the created queue on broker restarts.
        auto_delete: Delete queue after consumer is disconnected from
            broker.
        priorities: The number of priorities to support.
        extra_properties: Additional properties that will be added during
            queue creation.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    method_arguments: dict = {
        'type': 'queue',
        'name': queue_name,
        'properties': {
            'durable': durable,
            'auto-delete': auto_delete,
            'qpid.priorities': priorities
        }
    }

    if extra_properties:
        method_arguments['properties'].update(extra_properties)

    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    create_queue_message = create_QMF2_method_invoke(
        get_broker_id(server_url),
        'create', method_arguments)
    rpc.call(create_queue_message, timedelta(seconds=5))


def delete_queue(queue_name: str, server_url: Optional[str] = None):
    """Delete a queue on the AMQP broker.

    Args:
        queue_name: Name of queue.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    delete_queue_message = create_QMF2_method_invoke(
        get_broker_id(server_url),
        'delete', {
            'type': 'queue',
            'name': queue_name
        }
    )
    rpc.call(delete_queue_message, timedelta(seconds=5))


def _build_message_filter(key: str, value: str) -> dict:
    return {
        'filter_type': 'header_match_str',  # type: ignore
        'filter_params': {'header_key': key, 'header_value': value}
    }
