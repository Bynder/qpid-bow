from contextlib import suppress
from os import environ
from unittest import TestCase

from qpid_bow.config import configure, config, get_urls, process_url

class TestGetURLs(TestCase):
    def setUp(self):
        self.old_config = config
        self.old_env = environ.get('AMQP_SERVERS')

    def tearDown(self):
        config.clear()
        config.update(self.old_config)
        if self.old_env:
            environ['AMQP_SERVERS'] = self.old_env
        else:
            with suppress(KeyError):
                del environ['AMQP_SERVERS']

    def test_no_config(self):
        with suppress(KeyError):
            del environ['AMQP_SERVERS']

        with self.assertRaises(ValueError):
            get_urls()

    def test_get_urls_priority_from_environ(self):
        environ['AMQP_SERVERS'] = 'amqps://environ.example'
        self.assertEqual(get_urls(), ['amqps://environ.example'])

    def test_get_urls_priority_from_config(self):
        environ['AMQP_SERVERS'] = 'amqps://environ.example'
        configure({'amqp_url': 'amqps://config.example'})

        self.assertEqual(get_urls(), ['amqps://config.example'])

    def test_get_urls_priority_from_args(self):
        environ['AMQP_SERVERS'] = 'amqps://environ.example'
        configure({'amqp_url': 'amqps://config.example'})

        self.assertEqual(get_urls('amqps://args.example'),
                         ['amqps://args.example'])

    def test_get_urls_comma_seperated(self):
        self.assertEqual(
            get_urls('amqps://args1.example, amqps://args2.example'),
            ['amqps://args1.example', 'amqps://args2.example'])

    def test_get_urls_activemq_format(self):
        self.assertEqual(get_urls('amqp+ssl://args.example'),
                         ['amqps://args.example'])

    def test_get_urls_activemq_format_comma_seperated(self):
        self.assertEqual(
            get_urls('amqp+ssl://args1.example, amqp+ssl://args2.example'),
            ['amqps://args1.example', 'amqps://args2.example'])

    def test_get_urls_user_passwd_config_mixed(self):
        config['username'] = 'otheruser'
        config['password'] = 'otherpass'
        self.assertEqual(
            get_urls('amqps://args1.example,amqps://user:pass@args2.example'),
            ['amqps://otheruser:otherpass@args1.example',
             'amqps://user:pass@args2.example'])

    def test_process_url_noop(self):
        valid_url = 'amqp://some.example'
        self.assertEqual(process_url(valid_url), valid_url)

    def test_process_url_activemq(self):
        self.assertEqual(process_url('amqp+ssl://some.example'),
                         'amqps://some.example')

    def test_process_url_user_passwd_config(self):
        config['username'] = 'user'
        config['password'] = 'pass'
        self.assertEqual(process_url('amqps://some.example'),
                         'amqps://user:pass@some.example')

    def test_process_url_user_passwd_no_override(self):
        config['username'] = 'otheruser'
        config['password'] = 'otherpass'
        valid_url = 'amqps://user:pass@some.example'
        self.assertEqual(process_url(valid_url), valid_url)
