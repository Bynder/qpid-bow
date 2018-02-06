import logging
from argparse import (
    ArgumentParser,
    Namespace,
    _SubParsersAction,
)
from typing import (
    Callable,
    Iterable,
)

from qpid_bow.cli.connection_kill import connection_kill_parser
from qpid_bow.cli.message_receive import message_receive_parser
from qpid_bow.cli.message_send import message_send_parser
from qpid_bow.cli.queue_create import queue_create_parser
from qpid_bow.cli.queue_delete import queue_delete_parser
from qpid_bow.cli.queue_purge import queue_purge_parser
from qpid_bow.cli.queue_reroute import queue_reroute_parser
from qpid_bow.cli.queue_stats import queue_stats_parser
from qpid_bow.cli.route_dump import route_dump_parser
from qpid_bow.cli.route_config import route_config_parser
from qpid_bow.cli.session_outgoing import session_outgoing_parser


def main():
    logging.basicConfig(level=logging.CRITICAL)
    parser = ArgumentParser(prog='qb')
    parser.set_defaults(parser=parser)
    action = parser.add_subparsers(title='commands')

    create_command('queue', 'Manage queues', action,
                   (queue_create_parser, queue_delete_parser,
                    queue_purge_parser, queue_reroute_parser,
                    queue_stats_parser))

    create_command('message', 'Manage messages', action,
                   (message_receive_parser, message_send_parser))

    create_command('route', 'Manage routes', action,
                   (route_dump_parser, route_config_parser))

    create_command('connection', 'Manage connections', action,
                   (connection_kill_parser,))

    create_command('session', 'Manage sessions', action,
                   (session_outgoing_parser,))

    args: Namespace = parser.parse_args()
    if len(args.__dict__) <= 1:
        args.parser.print_help()
    else:
        args.func(args)


def create_command(command: str, helpline: str, action: _SubParsersAction,
                   subparsers: Iterable[Callable]):
    parser = action.add_parser(command, help=helpline)
    parser.set_defaults(parser=parser)
    action = parser.add_subparsers(title='commands')
    for subparser in subparsers:
        subparser(action)


if __name__ == '__main__':
    main()
