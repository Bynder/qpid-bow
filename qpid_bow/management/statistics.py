"""Gather statistics from AMQP broker."""

from datetime import timedelta
from typing import Optional

from proton import Message

from qpid_bow.management import (
    EXCHANGE_ID_PREFIX,
    create_QMF2_query,
)
from qpid_bow.remote_procedure import RemoteProcedure


def queue_statistics(queue_name: Optional[str] = None,
                     include_autodelete: bool = False,
                     server_url: Optional[str] = None) -> dict:
    """Retrieve total messages count and depth for all queues from AMQP broker.

    Args:
        queue_name: Name of queue.
        include_autodelete: Include autodelete queues to output.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: A dict mapping between queue address and dict with total messages
        and queue depth.

    Example:
        >>> queue_statistics(queue_name='examples')
        {'org.apache.qpid.broker:queue:examples': {'name': 'examples', 'total': 96, 'depth': 12}}
    """
    queues = {}

    def _update_queue_stats(message: Message):
        for item in message.body:
            values = item['_values']
            if values['autoDelete'] and not include_autodelete:
                continue  # We are not interested in temp reply queues

            current_queue_name = values['name'].decode()

            if queue_name and current_queue_name != queue_name:
                continue

            queues[item['_object_id']['_object_name'].decode()] = {
                'name': values['name'].decode(),
                'total': int(values['msgTotalEnqueues']),
                'depth': int(values['msgDepth'])
            }
        return True

    rpc = RemoteProcedure(_update_queue_stats, 'qmf.default.direct',
                          server_url)
    queue_query_message = create_QMF2_query('org.apache.qpid.broker',
                                            'queue')
    rpc.call(queue_query_message, timedelta(seconds=15))
    return queues


def exchange_statistics(server_url: Optional[str] = None) -> dict:
    """Retrieve total and dropped amount of messages for exchanges from AMQP broker.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: A dict mapping between exchange address and dict with exchange
            name, total messages count and dropped messages count.

    Example:
        >>> exchange_statistics()
        {'org.apache.qpid.broker:exchange:': {'name': '', 'total': ulong(236), 'dropped': ulong(0)}}
    """
    exchanges = {}

    def _update_exchange_stats(message: Message):
        for item in message.body:
            values = item['_values']
            name = values['name'].decode()
            if name.startswith(('qmf', 'qpid', 'amq')):
                continue  # Don't export Qpid/QMF related stats

            exchanges[item['_object_id']['_object_name'].decode()] = {
                'name': name,
                'total': values['msgReceives'],
                'dropped': values['msgDrops']
            }
        return True

    rpc = RemoteProcedure(_update_exchange_stats, 'qmf.default.direct',
                          server_url)
    exchange_query_message = create_QMF2_query('org.apache.qpid.broker',
                                               'exchange')
    rpc.call(exchange_query_message, timedelta(seconds=5))
    return exchanges


def gather_statistics(server_url: Optional[str] = None) -> dict:
    """Retrieve statistics about exchanges and queues from AMQP broker.

    Statistics data includes exchanges and queues. Exchange information
    includes exchange name, total and dropped amount of messages. Queue
    information includes messages count, depth and bindings to exchange.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: Exchange and queue statistics.

    Example:
        >>> gather_statistics()
        {'exchanges': {'org.apache.qpid.broker:exchange:': {'dropped': \
ulong(0), 'name': '', 'total': ulong(251)}}, 'queues': {\
'org.apache.qpid.broker:queue:examples': {'bindings': [{'exchange_id': \
'org.apache.qpid.broker:exchange:', 'name': 'default_route', 'total': 96}], \
'depth': 12, 'name': 'examples', 'total': 96}}}
    """
    stats = {
        'queues': queue_statistics(server_url=server_url),
        'exchanges': exchange_statistics(server_url),
    }

    for queue in stats['queues'].values():
        queue['bindings'] = []

    def _update_binding_stats(message: Message):
        for item in message.body:
            values = item['_values']
            queue_id = values['queueRef']['_object_name'].decode()
            exchange_id = values['exchangeRef']['_object_name'].decode()
            if queue_id not in stats['queues']:
                continue  # Filtered queue
            if exchange_id not in stats['exchanges']:
                continue  # Filtered exchange
            if exchange_id == EXCHANGE_ID_PREFIX:
                continue  # Default exchange stats are broken, reconstruct

            exchange_stats = {
                'name': values['bindingKey'].decode(),
                'exchange_id': exchange_id,
                'total': values['msgMatched']
            }
            stats['queues'][queue_id]['bindings'].append(exchange_stats)
        return True

    rpc = RemoteProcedure(_update_binding_stats, 'qmf.default.direct',
                          server_url)
    binding_query_message = create_QMF2_query('org.apache.qpid.broker',
                                              'binding')
    rpc.call(binding_query_message, timedelta(seconds=15))

    # Reconstruct default route stats
    for queue in stats['queues'].values():
        total_routed = sum((binding['total'] for binding in queue['bindings']))
        queue['bindings'].append({
            'name': 'default_route',
            'exchange_id': EXCHANGE_ID_PREFIX,
            'total': queue['total'] - total_routed
        })

    return stats
