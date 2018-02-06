"""AMQP broker exchange management."""

from collections import defaultdict
from copy import copy
from datetime import timedelta
from enum import Enum
from logging import getLogger
from typing import (
    MutableMapping,
    Optional,
    Set,
    Tuple,
)
from uuid import (
    NAMESPACE_URL,
    uuid5,
)

from proton import Message

from qpid_bow.management import (
    create_QMF2_method_invoke,
    create_QMF2_query,
    get_broker_id,
    get_object,
    handle_QMF2_exception,
)
from qpid_bow.remote_procedure import RemoteProcedure

logger = getLogger()
BINDING_NAME_TEMPLATE = 'binding://{exchange}/{queue}/{properties}'


class ExchangeType(Enum):
    """Define type of exchange."""
    direct = "direct"
    topic = "topic"
    fanout = "fanout"
    headers = "headers"


def create_exchange(exchange_name: str,
                    exchange_type: ExchangeType = ExchangeType.direct,
                    durable: bool = True,
                    server_url: Optional[str] = None):
    """Create an exchange on the broker.

    Args:
        exchange_name: Exchange name.
        exchange_type: `direct`, `topic`, `fanout`, `headers`.
        durable: Persist the created exchange on broker restarts.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    create_exchange_message = create_QMF2_method_invoke(
        get_broker_id(server_url),
        'create', {
            'type': 'exchange',
            'name': exchange_name,
            'properties': {
                'durable': durable,
                'exchange-type': exchange_type.value
            }
        }
    )
    rpc.call(create_exchange_message, timedelta(seconds=5))


def delete_exchange(exchange_name: str, server_url: Optional[str] = None):
    """Delete an exchange on the broker.

    Args:
        exchange_name: Exchange name.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    delete_exchange_message = create_QMF2_method_invoke(
        get_broker_id(server_url),
        'delete', {
            'type': 'exchange',
            'name': exchange_name
        }
    )
    rpc.call(delete_exchange_message, timedelta(seconds=5))


def create_binding(exchange_name: str, queue_name: str,
                   binding_name=None, headers_match: dict = None,
                   server_url: Optional[str] = None):
    """Create binding between queue and exchange.

    Args:
        exchange_name: Name of exchange.
        queue_name: Name of queue.
        binding_name: Name of binding.
        headers_match: Headers key-value pairs that should be presented on
            message to match the binding. Only for `headers` exchange type.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    exchange = get_object('org.apache.qpid.broker', 'exchange', exchange_name,
                          server_url)
    if headers_match and exchange['_values']['type'].decode() != 'headers':
        raise RuntimeError("Headers match only supported on headers exchange")

    method_arguments: MutableMapping = {
        'type': 'binding',
        'name': '{}/{}'.format(exchange_name, queue_name),
    }

    if binding_name:
        method_arguments['name'] = '{}/{}'.format(method_arguments['name'],
                                                  binding_name)

    if headers_match:
        method_arguments['properties'] = copy(headers_match)
        method_arguments['properties']['x-match'] = 'all'

    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    rpc.call(create_QMF2_method_invoke(get_broker_id(server_url),
                                       'create', method_arguments),
             timedelta(seconds=5))


def delete_binding(exchange_name: str, queue_name: str,
                   binding_name: str = None, server_url: Optional[str] = None):
    """Delete a binding on the broker.

    Args:
        exchange_name: Name of exchange.
        queue_name: Name of queue.
        binding_name: Name of binding.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.
    """
    rpc = RemoteProcedure(handle_QMF2_exception,
                          'qmf.default.direct', server_url)
    method_arguments = {
        'type': 'binding',
        'name': '{}/{}'.format(exchange_name, queue_name)
    }
    if binding_name:
        method_arguments['name'] = '{}/{}'.format(method_arguments['name'],
                                                  binding_name)
    delete_binding_message = create_QMF2_method_invoke(
        get_broker_id(server_url), 'delete', method_arguments
    )
    rpc.call(delete_binding_message, timedelta(seconds=5))


def get_exchange_bindings(server_url: Optional[str] = None) -> dict:
    """Retrieve all exchanges and bindings associated with these exchanges.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: A dict mapping between exchange it's bindings.

    Example:
        >>> get_exchange_bindings()
        {'org.apache.qpid.broker:exchange:': [\
{'queue_id': 'org.apache.qpid.broker:queue:examples', 'headers_match': {}}]}
    """
    results: defaultdict = defaultdict(list)

    def _update_bindings(message: Message):
        for item in message.body:
            logger.info("Got binding: %s", item)
            values = item['_values']

            exchange_id = values['exchangeRef']['_object_name'].decode()
            results[exchange_id].append({
                'queue_id': values['queueRef']['_object_name'].decode(),
                'headers_match': values.get('arguments')
            })

        return True

    rpc = RemoteProcedure(_update_bindings, 'qmf.default.direct',
                          server_url)
    binding_query_message = create_QMF2_query('org.apache.qpid.broker',
                                              'binding')
    rpc.call(binding_query_message, timedelta(seconds=5))
    return results


def get_binding_keys(exchange_name: str, queue_name: str = None,
                     server_url: Optional[str] = None
                    ) -> Set[Tuple[str, str, str]]:
    """Retrieve all bindings for specified exchange.

    Args:
        exchange_name: Name of exchange.
        queue_name: Name of queue.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        Set of binding keys.
    """
    result = set()

    def _filter_bindings(message: Message):
        for item in message.body:
            values = item['_values']
            queue_id = values['queueRef']['_object_name'].decode()
            qpid_queue_name = queue_id.rsplit(':', 1)[-1]
            exchange_id = values['exchangeRef']['_object_name'].decode()
            qpid_exchange_name = exchange_id.rsplit(':', 1)[-1]
            if exchange_name == qpid_exchange_name:
                if not queue_name or (queue_name == qpid_queue_name):
                    result.add((
                        qpid_exchange_name,
                        qpid_queue_name,
                        values['bindingKey'].decode()
                    ))
        return True

    rpc = RemoteProcedure(_filter_bindings, 'qmf.default.direct', server_url)
    binding_query_message = create_QMF2_query('org.apache.qpid.broker',
                                              'binding')
    rpc.call(binding_query_message, timedelta(seconds=5))
    return result


def get_headers_binding_name(exchange: str, queue_name: str,
                             headers_match: dict):
    """Generate UUID for exchange, queue and binding.

    Args:
        exchange: Name of exchange.
        queue_name: Name of queue.
        headers_match: Headers key-value pairs that should be presented on
            message to match the binding. Only for `headers` exchange type.

    Returns:
        UUID generated based on specified arguments.
    """
    binding = BINDING_NAME_TEMPLATE.format(
        exchange=exchange,
        queue=queue_name,
        properties='/'.join(
            '{key}/{value}'.format(key=str(key), value=str(value))
            for (key, value) in sorted(headers_match.items())
        )
    )
    return uuid5(NAMESPACE_URL, binding).hex
