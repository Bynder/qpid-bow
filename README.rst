qpid-bow
========

.. image:: https://travis-ci.org/Bynder/qpid-bow.svg?branch=master
    :target: https://travis-ci.org/Bynder/qpid-bow
.. image:: https://coveralls.io/repos/github/Bynder/qpid-bow/badge.svg?branch=master
    :target: https://coveralls.io/github/Bynder/qpid-bow?branch=master
.. image:: https://readthedocs.org/projects/qpid-bow/badge/?version=latest
    :target: https://qpid-bow.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Qpid Bow is a higher level client framework for Python 3.6+ to communicate with
AMQP/Qpid servers combined with a set of CLI tools to manage a Qpid server.

With its CLI tools Qpid Bow provides the missing tooling you always wanted
to administrate or debug your Qpid-based AMQP stack. Providing you with the
power to manage queues and exchanges, setup and save routing using YAML files
and various other tools.

As a framework Qpid Bow can provide you with a higher level interface on top of
the low level Qpid Proton library to integrate with AMQP/Qpid queues,
exchanges and Remote Procedure Call (RPC) functionality:

* Simple, callback based receiver, supporting listening for multiple queues.
* RPC calls with automatic temporary queues and callbacks.
* Queue based sender.
* Included Qpid management code for queue/exchange creation.
* Support to run under Python's asyncio event loop with *async def* callbacks.


Requirements
------------

* Python 3.6+
* `python-qpid-proton <https://pypi.python.org/pypi/python-qpid-proton>`_
* `PyYAML <https://pypi.python.org/pypi/PyYAML>`_


Installation
------------
Qpid Bow is available from PyPI:

    $ pip install qpid-bow

Or add qpid-bow to your application's requirements using
``requirements.txt`` / ``setup.py`` / ``Pipfile``.


Testing
-------
Qpid Bow's unit tests need to connect to an actual Apache Qpid server for all
tests to succeed. By default the tests assume a server exists on localhost.

To specify the server address to use for tests use the environment variable:
``AMQP_TEST_SERVERS``


Available tools
---------------

**Queue**

* ``qb queue create`` - Create queues.
* ``qb queue delete`` - Delete queues.
* ``qb queue purge`` - Purge messages from a queue.
* ``qb queue reroute`` - Reroute messages from a queue to an exchange.
* ``qb queue stats`` - Print queue usage statistics and active number of messages.


**Message**

* ``qb message receive`` - Receive messages from a queue or an exchange.
* ``qb message send`` - Send messages to a queue or an exchange.


**Route**

* ``qb route dump`` - View & save exchange -> queue routing.
* ``qb route config`` - Setup exchange -> queue routing from a saved file.


**Connection**

* ``qb connection kill`` - Kill connections from the server.


**Session**

* ``qb session outgoing`` - List outgoing sessions from the server.


Environment variables
---------------------

``AMQP_SERVERS`` - comma-separated list of main and failover servers to connect to

``AMQP_TEST_SERVERS`` - Same as ``AMQP_SERVERS``, used solely for unittests

example: ``AMQP_SERVERS=amqp://user:pass@192.168.1.1:5672,amqp://user:pass@192.168.1.2:5672``
