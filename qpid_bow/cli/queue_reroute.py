from os import EX_USAGE

from qpid_bow.management.queue import reroute_queue


def queue_reroute_parser(action):
    parser = action.add_parser(
        'reroute', help="Reroute messages from a queue to an exchange, "
                        "optionally filtering")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=queue_reroute)

    parser.add_argument('queue', metavar='QUEUE', type=str,
                        help="Queue address")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-e', '--exchange-address', type=str, required=False,
                        help="Exchange address, if not provided"
                             "alternative exchange will be used")
    parser.add_argument('-l', '--limit', type=int, required=False, default=0,
                        help="Limit the number of messages")
    parser.add_argument('-f', '--filter', type=str, default=None, nargs='?',
                        help="Filter based on property=value")


def queue_reroute(args):
    try:
        filter_key, filter_value = args.filter.split('=', 1)
        filter_tuple = (filter_key, filter_value)
    except ValueError:
        print("Invalid filter specified\n")
        args.parser.print_help()
        exit(EX_USAGE)
    except AttributeError:
        filter_tuple = None

    reroute_queue(args.queue_address, args.exchange_address, args.limit,
                  filter_tuple, args.broker_url)
