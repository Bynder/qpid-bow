import asyncio

from proton import Message
from qpid_bow.asyncio import Container as AsyncioContainer
from qpid_bow.exc import QMF2Exception, QMF2ObjectExists
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message, create_reply
from qpid_bow.receiver import Receiver
from qpid_bow.sender import Sender

SERVER_URL = '127.0.0.1'
QUEUE_NAME = 'examples'
REPLY_QUEUE_NAME = 'examples_reply'
REPLY_SENDER = Sender(server_url=SERVER_URL)


def main():
    try_create_queue(QUEUE_NAME)
    try_create_queue(REPLY_QUEUE_NAME)

    send_messages()

    receiver = Receiver(consumer, QUEUE_NAME, SERVER_URL,
                        container_class=AsyncioContainer)

    reply_receiver = Receiver(reply_consumer, REPLY_QUEUE_NAME, SERVER_URL,
                              container_class=AsyncioContainer)
    receiver.run()
    reply_receiver.run()
    loop = asyncio.get_event_loop()
    loop.run_forever()


def send_messages():
    sender = Sender(QUEUE_NAME, SERVER_URL)
    messages = []
    for num in reversed(range(3)):
        message = create_message(f'Test message {num+1}')
        message.address = QUEUE_NAME
        message.reply_to = REPLY_QUEUE_NAME
        messages.append(message)
    sender.queue(messages)
    sender.send()


async def consumer(message: Message) -> bool:
    print(f'Received message: {message.body}')
    reply_message = create_reply(message, f'Reply for "{message.body}"')
    REPLY_SENDER.queue((reply_message,))
    REPLY_SENDER.send()
    return True


async def reply_consumer(message: Message) -> bool:
    print(f'Received reply message: {message.body}')

    return True


def try_create_queue(queue_name: str):
    try:
        create_queue(queue_name, priorities=10, server_url=SERVER_URL)
    except QMF2ObjectExists:
        pass  # Queue already exists
    except QMF2Exception:
        print("QMF2 error during queue creation")
        raise


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
