from qpid_bow.receiver import Receiver


def message_receive_parser(action):
    parser = action.add_parser(
        'receive', help="Receive a message")
    parser.set_defaults(parser=parser)
    parser.set_defaults(func=message_receive)

    parser.add_argument('address', metavar='ADDRESS', type=str,
                        help="Queue or exchange address from where to receive "
                             "a message")
    parser.add_argument('-b', '--broker-url', type=str, required=False,
                        help="amqp:// or amqps:// URL to the broker")
    parser.add_argument('-c', '--count', type=int, required=False, default=1,
                        help="How many messages to receive")


def message_receive(args):
    def callback(message):
        print(message.body)
        return True

    receiver = Receiver(callback, args.address, args.broker_url, args.count)
    receiver.receive()
