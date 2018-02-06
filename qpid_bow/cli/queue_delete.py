from os import EX_USAGE

from qpid_bow.management.queue import delete_queue
from qpid_bow.exc import (
    QMF2Forbidden,
    QMF2NotFound,
    TimeoutReached,
)


def queue_delete_parser(action):
    parser = action.add_parser(
        'delete', help="Delete queue")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=queue_delete)

    parser.add_argument('queue', metavar='QUEUE', type=str,
                        help="Queue address")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")


def queue_delete(args):
    try:
        delete_queue(args.queue, args.broker_url)
    except ValueError as e:
        print(str(e) + '\n')
        args.parser.print_help()
        exit(EX_USAGE)
    except QMF2NotFound:
        print(f"Queue '{args.queue}' not found")
        exit(EX_USAGE)
    except QMF2Forbidden:
        print('Not enough permissions to delete queue')
        exit(EX_USAGE)
    except TimeoutReached:
        print('Timeout, server is not available or wrong login/password')
        exit(EX_USAGE)
