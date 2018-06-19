from os import environ

TEST_AMQP_SERVER = environ.get('AMQP_TEST_SERVERS', 'amqp://127.0.0.1')
