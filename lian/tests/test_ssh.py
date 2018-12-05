# -*- coding: utf-8 -*-

import logging

from lian import settings
from lian.ssh import SSHChain, SSHConnection

logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)
LOG = logging.getLogger(__name__)

TEST_LOCALHOST_HOST = 'localhost'
TEST_LOCALHOST_USER = 'root'
TEST_LOCALHOST_PASS = '123456'


def _execute(conn, command):
    code, out, err = conn.execute(command)
    LOG.info('ExitCode: %s', code)
    LOG.info('StdOut: %s', out)
    LOG.info('StdErr: %s', err)


def test_conn():
    conn = SSHConnection(host=TEST_LOCALHOST_HOST, username=TEST_LOCALHOST_USER, password=TEST_LOCALHOST_PASS).connect()
    _execute(conn, 'uname -a')


def test_chains():
    with SSHChain() as chain:
        chain.append(host=TEST_LOCALHOST_HOST, username=TEST_LOCALHOST_USER, password=TEST_LOCALHOST_PASS)
        _execute(chain, 'w')

        chain.append(host=TEST_LOCALHOST_HOST, username=TEST_LOCALHOST_USER, password=TEST_LOCALHOST_PASS)
        _execute(chain, 'w')

        chain.append(host=TEST_LOCALHOST_HOST, username=TEST_LOCALHOST_USER, password=TEST_LOCALHOST_PASS)
        _execute(chain, 'w')

        chain.append(host=TEST_LOCALHOST_HOST, username=TEST_LOCALHOST_USER, password=TEST_LOCALHOST_PASS)
        _execute(chain, 'w')


if __name__ == '__main__':
    test_chains()
