from argparse import FileType
from os import (
    EX_DATAERR,
    EX_USAGE,
)

import yaml

from qpid_bow.exc import (
    ObjectNotFound,
    QMF2ObjectExists,
)
from qpid_bow.management.exchange import (
    create_binding,
    create_exchange,
    delete_binding,
    get_binding_keys,
    get_headers_binding_name,
    ExchangeType,
)
from qpid_bow.management.queue import create_queue


def route_config_parser(action):
    parser = action.add_parser(
        'config', help="Configure exchange routing")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=route_config)

    parser.add_argument('file', metavar='FILE', type=FileType('r'),
                        help="Mapping configuration file (YAML)")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-f', "--force-creation",
                        action='store_true', default=False,
                        help="Force creation of missing exchanges and queues")


def route_config(args):
    try:
        route_mapping = yaml.safe_load(args.file)
    except yaml.YAMLError:
        print("Invalid YAML config mapping\n")
        args.parser.print_help()
        exit(EX_DATAERR)

    for exchange in route_mapping:
        valid_bindings = set()
        if args.force_creation:
            try:
                create_exchange(exchange, ExchangeType.headers,
                                server_url=args.broker_url)
            except QMF2ObjectExists:
                print("Exchange {} already exists, moving on..."
                      .format(exchange))
        for queue in route_mapping[exchange]:
            if args.force_creation:
                try:
                    create_queue(queue, server_url=args.broker_url)
                except QMF2ObjectExists:
                    print("Queue {} already exists, moving on..."
                          .format(queue))
            for binding in route_mapping[exchange][queue]:
                binding_name = get_headers_binding_name(
                    exchange, queue, binding)
                valid_bindings.add((exchange, queue, binding_name))
                # A binding with an existing name will be updated.
                # Since we generate the name from the header match properties,
                # this is equivalent to ignoring.
                try:
                    create_binding(exchange, queue,
                                   headers_match=binding,
                                   binding_name=binding_name,
                                   server_url=args.broker_url)
                except ValueError as e:
                    print(str(e) + '\n')
                    args.parser.print_help()
                    exit(EX_USAGE)
                except ObjectNotFound as e:
                    print(str(e) + '\n')
                    args.parser.print_help()
                    exit(EX_USAGE)

        binding_keys = get_binding_keys(
            exchange, server_url=args.broker_url
        )
        # Delete old bindings
        for binding_key in binding_keys - valid_bindings:
            delete_binding(binding_key[0], binding_key[1], binding_key[2],
                           server_url=args.broker_url)
