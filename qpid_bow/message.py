"""Message utility methods."""

from copy import copy
from typing import Optional, Union

from proton import Message

from qpid_bow import Priority
from qpid_bow.exc import UnroutableMessage


def decode_message(data: bytes) -> Message:
    """Utility method to decode message from bytes.

    Args:
        data: Raw AMQP data in bytes.

    Returns:
        Message: Decoded message.
    """
    message = Message()
    message.decode(data)
    return message


def create_message(body: Union[str, bytes, dict, list],
                   properties: Optional[dict] = None,
                   priority: Priority = Priority.normal) -> Message:
    """Utility method to create message with common attributes.

    Args:
        body: Message body.
        properties: Message properties.
        priority: Message priority.

    Returns:
        Message: Created message.
    """
    message = Message(body=body, durable=True, priority=priority.value,
                      properties=properties or {})
    if isinstance(body, bytes):
        message.content_type = 'application/octet-stream'
    else:
        message.content_type = None

    return message


def create_reply(origin_message: Message,
                 result_data: Union[str, bytes, dict, list]) -> Message:
    """Create reply to origin message with result data.

       Reply messages share the same correlation ID, properties and priority
       with the exception of being marked as reply.

       The address is set to the reply_to address from the origin message
       for usage in a addressless Sender.

    Args:
        origin_message: Origin message we are replying to.
        result_data: Message body of the reply.

    Returns:
        Message: Created reply message.
    """

    if not origin_message.reply_to:
        raise UnroutableMessage("Origin message has no reply-to address")

    reply_message = create_message(result_data,
                                   copy(origin_message.properties),
                                   Priority(origin_message.priority))
    reply_message.address = origin_message.reply_to
    reply_message.correlation_id = origin_message.correlation_id
    reply_message.properties['is_reply'] = True
    return reply_message
