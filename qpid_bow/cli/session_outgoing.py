import re
from os import EX_USAGE

from qpid_bow.management.session import (
    get_outgoing_sessions_by_address,
    get_sessions,
)


def session_outgoing_parser(action):
    parser = action.add_parser(
        'outgoing', help="Dump outgoing sessions")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=session_outgoing)

    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-f', '--filter', type=re.compile, required=False,
                        default=None, help="Filter by regex pattern")
    parser.add_argument('-p', '--pretty',
                        required=False, default=False,
                        action='store_true', help="Pretty output")


def session_outgoing(args):
    try:
        sessions = get_sessions(args.broker_url)
    except ValueError as e:
        print(str(e) + '\n')
        args.parser.print_help()
        exit(EX_USAGE)

    outgoing_sessions_by_address = get_outgoing_sessions_by_address(
        args.broker_url)
    for address_name, outgoing_sessions in \
            outgoing_sessions_by_address.items():
        if args.filter and not args.filter.match(address_name):
            continue

        if args.pretty:
            print('╓\n║ Address: {}'.format(address_name))

        num_sessions = len(outgoing_sessions)
        for index, outgoing_session in enumerate(outgoing_sessions):
            last_item = index == num_sessions - 1
            line_char = '┖─' if last_item else '┠─'
            if outgoing_session['session_id'] not in sessions:
                continue
            session = sessions[outgoing_session['session_id']]

            if args.pretty:
                print('║ {}{}'.format(line_char,
                                      session['address']))
                print('║ {}\t Transfers: {}'.format(
                    '' if last_item else '┃',
                    outgoing_session['transfers']))
            else:
                print(session['address'])
        if args.pretty:
            print('╙')
