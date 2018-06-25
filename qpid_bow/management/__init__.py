from datetime import timedelta
from typing import Any, Mapping, Optional

from proton import Message

from qpid_bow.exc import (
    ObjectNotFound,
    QMF2Exception,
)
from qpid_bow.remote_procedure import RemoteProcedure

QUEUE_ID_PREFIX = 'org.apache.qpid.broker:queue:'
EXCHANGE_ID_PREFIX = 'org.apache.qpid.broker:exchange:'


def create_QMF2_message() -> Message:
    """Factory function to create a base message for a QMF2 RPC call.

    Returns:
        Message: A base message for a QMF2 RPC call.
    """
    return Message(subject='broker', properties={
        'routing-key': 'broker',
        'x-amqp-0-10.app-id': 'qmf2',
        'method': 'request'
    })


def create_QMF2_query(package_name: str, class_name: str) -> Message:
    """Factory function to create a QMF2 object query.

    Args:
        package_name: Qpid internal package name to query.
        class_name: Qpid internal class name to query.

    Returns:
        Message: A message fully setup with a QMF2 object query RPC call.
    """
    message = create_QMF2_message()
    message.correlation_id = class_name
    message.properties['qmf.opcode'] = '_query_request'
    message.body = {
        '_what': 'OBJECT',
        '_schema_id': {
            '_package_name': package_name,
            '_class_name': class_name
        }
    }

    return message


def create_QMF2_method_invoke(object_id: dict,
                              method_name: str,
                              arguments: Mapping[str, Any]) -> Message:
    """Factory function to create a QMF2 object method call.

    Args:
        object_id: Qpid internal object ID.
        method_name: Name of the method to call.
        arguments: Mapping with key/value pairs of arguments for the method.

    Returns:
        Message: A message fully setup with a QMF2 method invoke RPC call.
    """
    message = create_QMF2_message()
    message.corrrelation_id = 'method'
    message.properties['qmf.opcode'] = '_method_request'
    message.body = {
        '_object_id': object_id,
        '_method_name': method_name,
        '_arguments': arguments
    }

    return message


def handle_QMF2_exception(message: Message):
    """Deserialises and raises a QMF2 exception from a reply, in case the
    QMF2 RPC call failed.

    Args:
        message: The QMF2-RPC reply message.
    """
    if message.properties.get('qmf.opcode') != '_exception':
        return True

    raise QMF2Exception.from_data(message.body['_values'])


def get_object(package_name: str, class_name: str, object_name: str,
               server_url: Optional[str] = None) -> dict:
    """Find a raw QMF2 object by type and name.

    Args:
        package_name: Qpid internal package name to query.
        class_name: Qpid internal class name to query.
        object_name: Name of the Qpid object to find.
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: Raw QMF2 object.
    """

    object_: dict = {}

    def handle_response(message: Message):
        for result in message.body:
            if result['_values']['name'].decode() == object_name:
                object_.update(result)
                return True

        return True

    rpc = RemoteProcedure(handle_response,
                          'qmf.default.direct', server_url)
    rpc.call(create_QMF2_query(package_name, class_name),
             timedelta(seconds=5))

    if not object_:
        raise ObjectNotFound(class_name, object_name)

    return object_


def get_broker_id(server_url: Optional[str] = None) -> dict:
    """Get the full internal broker ID object.

    Args:
        server_url: Comma-separated list of urls to connect to.
            Multiple can be specified for connection fallback, the first
            should be the primary server.

    Returns:
        dict: Full internal broker ID object.
    """
    broker_id: dict = {}

    def handle_response(message: Message):
        broker_id.update(message.body[0]['_object_id'])
        return True

    rpc = RemoteProcedure(handle_response, 'qmf.default.direct', server_url)
    broker_query_message = create_QMF2_query('org.apache.qpid.broker',
                                             'broker')
    rpc.call(broker_query_message, timedelta(seconds=5))
    return broker_id
