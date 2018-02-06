from os import EX_USAGE

from qpid_bow.management.queue import create_queue
from qpid_bow.exc import (
    QMF2NotFound,
    QMF2ObjectExists,
    TimeoutReached,
)


def queue_create_parser(action):
    parser = action.add_parser(
        'create', help="Create queue")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=queue_create)

    parser.add_argument('queue', metavar='QUEUE', type=str,
                        help="Queue address")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-d', '--durable',
                        action='store_true', default=False,
                        help="Persist on broker restart")
    parser.add_argument('-p', '--priorities', type=int, required=False,
                        default=0,
                        help="Amount of priorities in queue, default: 0")


def queue_create(args):
    try:
        create_queue(args.queue, args.durable, priorities=args.priorities,
                     server_url=args.broker_url)
    except ValueError as e:
        print(str(e) + '\n')
        args.parser.print_help()
        exit(EX_USAGE)
    except QMF2ObjectExists:
        print(f"Queue '{args.queue}' already exist")
        exit(EX_USAGE)
    except QMF2NotFound:
        print('Object not found, probably not enough permissions to create '
              'queue')
        exit(EX_USAGE)
    except TimeoutReached:
        print('Timeout, server is not available or wrong login/password')
        exit(EX_USAGE)
