import asyncio

from proton import Message
from qpid_bow.asyncio import Container as AsyncioContainer
from qpid_bow.exc import QMF2Exception, QMF2ObjectExists
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message
from qpid_bow.receiver import Receiver
from qpid_bow.sender import Sender

SERVER_URL = '127.0.0.1'
QUEUE_NAME = 'examples'


def main():
    try_create_queue(QUEUE_NAME)

    send_messages()

    receiver = Receiver(consumer, QUEUE_NAME, SERVER_URL,
                        container_class=AsyncioContainer)
    receiver.run()
    loop = asyncio.get_event_loop()
    loop.run_forever()


def send_messages():
    sender = Sender(server_url=SERVER_URL)
    messages = []
    for num in reversed(range(3)):
        message = create_message(f'Test message {num+1}')
        message.address = QUEUE_NAME
        messages.append(message)
    sender.queue(messages)
    sender.send()


async def consumer(message: Message) -> bool:
    print(f'Received message: {message.body}')
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
