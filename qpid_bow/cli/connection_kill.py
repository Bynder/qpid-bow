from os import EX_USAGE

from qpid_bow.management.connection import (
    get_connection_ids,
    kill_connection,
)


def connection_kill_parser(action):
    parser = action.add_parser(
        'kill', help="Kills connection")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=connection_kill)

    parser.add_argument('connections', metavar='CONNECTIONS', type=str,
                        nargs='+', help="Connections to kill, format: IP:PORT")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('--commit', action='store_true',
                        default=False, help="Commit to this action")


def connection_kill(args):
    connection_ids = []
    try:
        possible_connection_ids = get_connection_ids(args.broker_url)
    except ValueError as e:
        print(str(e) + '\n')
        args.parser.print_help()
        exit(EX_USAGE)

    for possible_connection_id in possible_connection_ids:
        for requested_connection in args.connections:
            if possible_connection_id.decode().endswith(requested_connection):
                connection_ids.append(possible_connection_id)

    print("{} kill connections:".format('Will' if args.commit else 'Would'))
    for connection_id in connection_ids:
        print(connection_id.decode())
        if args.commit:
            kill_connection({'_object_name': connection_id})
