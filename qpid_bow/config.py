"""Configure qpid-bow."""

from os import environ
from typing import (
    List,
    Mapping,
    Optional,
)

from urllib.parse import urlsplit, urlunsplit

config: dict = {}


def configure(new_config: Mapping):
    """Updates global config with provided mapping.

    Args:
        new_config: Mapping with config data.
    """
    config.update(new_config)


def process_url(url: str) -> str:
    """Processes a URL for usage with Qpid Proton.

    - ActiveMQ amqp+ssl scheme is replaced with amqps.
    - Adds username and password from config.

    Args:
        url: Input URL.

    Returns:
        str: Processed URL.
    """
    split_url = urlsplit(url.strip())
    if split_url.scheme == 'amqp+ssl':
        split_url = split_url._replace(scheme='amqps')

    if ((not split_url.username or not split_url.password) and
            'username' in config and 'password' in config):
        user_pass = f"{config['username']}:{config['password']}@"
        new_netloc = user_pass + split_url.netloc
        split_url = split_url._replace(netloc=new_netloc)

    return urlunsplit(split_url)


def get_urls(argument_urls: Optional[str] = None) -> List[str]:
    """Retrieves server argument_urls from one of the sources.

    The sources priority comes in the following order: passed arguments,
    global config, AMQP_SERVERS environment variable.

    Args:
        argument_urls: Comma-separated argument_urls.

    Returns:
        List[str]: Returns list of argument_urls to connect to.
    """
    if argument_urls:
        raw_urls = argument_urls
    elif 'amqp_url' in config:
        raw_urls = config['amqp_url']
    elif 'AMQP_SERVERS' in environ:
        raw_urls = environ['AMQP_SERVERS']
    else:
        raise ValueError('AMQP server url is not configured')

    return [process_url(url) for url in raw_urls.split(',')]
