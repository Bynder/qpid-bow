import re

from qpid_bow.management.statistics import queue_statistics


def queue_stats_parser(action):
    parser = action.add_parser('stats', help="Shows queue statistics")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=queue_stats)

    parser.add_argument('-q', '--queue', type=str, required=False,
                        default=None, help="Queue address")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-f', '--filter', type=re.compile, required=False,
                        default=None, help="Filter by regex pattern")
    parser.add_argument('-a', '--autodelete',
                        action='store_true', default=False,
                        help="Include autodelete (temporary) queues")


def queue_stats(args):
    first = True
    line = "{name}\n\tMessage depth: {depth}\n\tTotal enqueued:{total}"

    try:
        queues = queue_statistics(args.queue, args.autodelete, args.broker_url)
    except ValueError as e:
        print(str(e) + '\n')
        args.parser.print_help()
        return

    for q_info in queues.values():
        if args.filter and not args.filter.match(q_info['name']):
            continue

        if first:
            first = False
        else:
            print('')  # newline
        print(line.format(**q_info))
