import json
from argparse import FileType
from itertools import repeat
from os import EX_DATAERR

from qpid_bow.message import create_message
from qpid_bow.sender import Sender


def message_send_parser(action):
    parser = action.add_parser(
        'send', help="Send one or multiple messages")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=message_send)

    parser.add_argument('address', metavar='ADDRESS', type=str,
                        help="Queue or exchange address to send a message to")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-r', '--repeat', type=int, required=False, default=1,
                        help="Send same message several times")
    parser.add_argument('-p', '--properties-file', type=FileType('r'),
                        required=False, default=None,
                        help="Message properties file (JSON)")
    parser.add_argument('message', type=str, help="Message to send")


def message_send(args):
    properties = None
    if args.properties_file:
        try:
            properties = json.load(args.properties_file)
        except json.JSONDecodeError:
            print("Invalid JSON properties file\n")
            args.parser.print_help()
            exit(EX_DATAERR)

    message = create_message(args.message.encode(), properties)
    sender = Sender(args.address, args.broker_url)
    sender.queue(repeat(message, args.repeat))
    sender.send()
