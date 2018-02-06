from datetime import timedelta
from threading import Thread
from time import sleep

from proton import Message
from qpid_bow.exc import QMF2Exception, QMF2ObjectExists
from qpid_bow.management.queue import create_queue
from qpid_bow.message import create_message, create_reply
from qpid_bow.receiver import Receiver
from qpid_bow.remote_procedure import RemoteProcedure
from qpid_bow.sender import Sender

SERVER_URL = '127.0.0.1'
QUEUE_NAME = 'examples'
TIMEOUT = timedelta(seconds=5)
REPLY_SENDER = Sender(server_url=SERVER_URL)


def main():
    try_create_queue(QUEUE_NAME)

    remote_service = RemoveService()
    remote_service.start()

    message = create_message(f'Test RPC message')

    rpc = RemoteProcedure(rpc_callback, QUEUE_NAME, SERVER_URL)
    rpc.call(message, TIMEOUT)

    sleep(3)
    remote_service.receiver.stop()


def rpc_callback(message: Message) -> bool:
    print(f'Caller: received RPC result "{message.body}"')
    return True


def try_create_queue(queue_name: str):
    try:
        create_queue(queue_name, priorities=10, server_url=SERVER_URL)
    except QMF2ObjectExists:
        pass  # Queue already exists
    except QMF2Exception:
        print("QMF2 error during queue creation")
        raise


class RemoveService(Thread):
    def __init__(self):
        super().__init__()
        self.receiver = Receiver(self.remote_service_worker, QUEUE_NAME,
                                 SERVER_URL)

    def run(self):
        self.receiver.run()

    @staticmethod
    def remote_service_worker(message: Message) -> bool:
        print(
            f'Remote service: received RPC message "{message.body}", sending '
            f'result back')
        reply_message = create_reply(message, f'Test RPC response"')
        REPLY_SENDER.queue((reply_message,))
        REPLY_SENDER.send()
        return True


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
