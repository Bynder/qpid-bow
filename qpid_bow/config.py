"""Configure qpid-bow."""

from os import environ
from typing import (
    List,
    Mapping,
    Optional,
)

config: dict = {}


def configure(new_config: Mapping):
    """Updates global config with provided mapping.

    Args:
        new_config: Mapping with config data.
    """
    config.update(new_config)


def get_urls(urls: Optional[str] = None) -> List[str]:
    """Retrieves server urls from one of the sources.

    The sources priority comes in the following order: passed arguments,
    global config, AMQP_SERVERS environment variable.

    Args:
        urls: Comma-separated urls.

    Returns:
        List[str]: Returns list of urls to connect to.
    """
    if urls:
        return [url.strip() for url in urls.split(',')]

    if config.get('amqp_url'):
        return [url.strip() for url in config['amqp_url'].split(',')]

    amqp_servers = environ.get('AMQP_SERVERS')

    if amqp_servers:
        environ_urls = []
        for server in amqp_servers.split(','):
            environ_urls.append(server.strip())
        return environ_urls

    raise ValueError('AMQP server url is not configured')
