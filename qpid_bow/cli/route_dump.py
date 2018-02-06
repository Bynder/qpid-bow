import re

import yaml

from qpid_bow.management import (
    EXCHANGE_ID_PREFIX,
    QUEUE_ID_PREFIX,
)
from qpid_bow.management.exchange import get_exchange_bindings


def route_dump_parser(action):
    parser = action.add_parser(
        'dump', help="Dump exchange route bindings")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=route_dump)

    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-f', '--filter', type=re.compile, required=False,
                        default=None, help="Filter by regex pattern")
    parser.add_argument('-y', '--yaml', action='store_true', default=False,
                        help="Output as mapping configuration file")


def route_dump(args):
    try:
        exchange_bindings = get_exchange_bindings(args.broker_url)
    except ValueError as e:
        print(str(e) + '\n')
        args.parser.print_help()
        return

    if args.yaml:
        print(_get_yaml(args, exchange_bindings))
        return

    for exchange_id, bindings in exchange_bindings.items():
        exchange_name = _id_to_name(exchange_id)
        if args.filter and not args.filter.match(exchange_name):
            continue

        print('╓\n║ Exchange: {}'.format(exchange_name))

        num_bindings = len(bindings)
        for index, binding in enumerate(bindings):
            last_item = index == num_bindings - 1
            line_char = '┖─' if last_item else '┠─'
            print('║ {}{}'.format(line_char, _id_to_name(binding['queue_id'])))
            headers_match = binding.get('headers_match')
            if headers_match:
                print('║ {}\t Header match: {}'.format(
                    '' if last_item else '┃',
                    headers_match))
        print('╙')


def _get_yaml(args, exchange_bindings):
    result = {}
    for exchange_id, bindings in exchange_bindings.items():
        exchange_name = _id_to_name(exchange_id)

        if not exchange_name:
            continue

        result[exchange_name] = {}

        if args.filter and not args.filter.match(exchange_name):
            continue

        for binding in bindings:
            queue_name = _id_to_name(binding['queue_id'])
            headers_match: dict = binding.get('headers_match')
            if headers_match:
                if 'x-match' in headers_match:
                    del headers_match['x-match']
                if queue_name not in result[exchange_name]:
                    result[exchange_name][queue_name] = []
                result[exchange_name][queue_name].append(headers_match)

    return yaml.dump(result, default_flow_style=False)


def _id_to_name(value: str) -> str:
    if value.startswith(QUEUE_ID_PREFIX):
        prefix = QUEUE_ID_PREFIX
    else:
        prefix = EXCHANGE_ID_PREFIX
    return value.split(prefix, 1)[1]
