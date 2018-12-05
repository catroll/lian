# -*- coding: utf-8 -*-

import logging
import os
import time

import paramiko

from lian import fs
from lian import random_string

LOG = logging.getLogger(__name__)


class SSHConnection(object):
    def __init__(self, host, port=22, username='root', password=None, key_filename=None):
        assert isinstance(port, int), 'port must be integer: %r' % port

        self.alias = random_string.generate_name()

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.transport = None
        self.sftp = None
        self.channels = {}

        self.key_filename = key_filename
        if self.key_filename:
            LOG.info('[%s] Found private key: %s', self.alias, self.key_filename)
            key_path = os.path.expanduser('~/.ssh/%s' % self.key_filename)
            with open(key_path) as f:
                self.key = paramiko.RSAKey.from_private_key(f)
        else:
            self.key = None

        self.created_dirs = []

    def connect(self, via=None):
        LOG.info('[%s] connecting...', self.alias)
        if via:
            assert isinstance(via, SSHConnection), \
                '[%s] via %r is not instance of SSHConnection' % (self.alias, via)
            LOG.info('[%s] Connecting to %s@%s:%d (via %s)',
                     self.alias, self.username, self.host, self.port, via.host)
            chan = via.channel((self.host, self.port))
            tran = paramiko.Transport(chan)
        else:
            LOG.info('[%s] Connecting to %s@%s:%d', self.alias, self.username, self.host, self.port)
            tran = paramiko.Transport((self.host, self.port))

        # tran.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        tran.start_client()
        if not tran.is_authenticated() and self.username:
            if self.key:
                LOG.info('[%s] Trying login %s:%d as %s (key: %s)',
                         self.alias, self.host, self.port, self.username, self.key_filename)
                tran.auth_publickey(self.username, self.key)
            elif self.password:
                LOG.info('[%s] Trying login %s:%d as %s (use password)',
                         self.alias, self.host, self.port, self.username)
                tran.auth_password(self.username, self.password)

        self.transport = tran
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

        LOG.info('[%s] Is this connection authenticated? %r', self.alias, self.transport.is_authenticated())
        return self

    def channel(self, host):
        if not self.transport:
            self.connect()

        if host not in self.channels:
            # setup forwarding from 127.0.0.1:<free_random_port> to host
            kind = 'direct-tcpip'
            dest_addr = host
            src_addr = ('127.0.0.1', 0)
            LOG.info('[%s] [%s:%s] Open channel: kind %r, src_addr %r => dest_addr %r',
                     self.alias, self.host, self.port, kind, src_addr, dest_addr)
            chan = self.transport.open_channel(kind, dest_addr, src_addr)
            self.channels[host] = chan
        return self.channels[host]

    def execute(self, command, stdin=None, timeout=-1, buff_size=1024):
        assert isinstance(command, str), '[%s] command must be a string, but got %r' % (self.alias, command)
        LOG.info('[%s] [%s:%s] Execute command: %s', self.alias, self.host, self.port, command)

        channel = self.transport.open_session()

        out = b''
        err = b''
        pause = 0.1

        # from contextlib import closing
        # with closing(chan) as channel:

        # channel.set_combine_stderr(True)
        channel.settimeout(timeout)
        channel.exec_command(command)
        if stdin:
            channel.sendall(stdin)
            channel.shutdown_write()

        while not channel.exit_status_ready():
            LOG.debug('[%s] wait %s seconds...', self.alias, pause)
            time.sleep(pause)
            if channel.recv_ready():
                out += channel.recv(buff_size)
            if channel.recv_stderr_ready():
                err += channel.recv_stderr(buff_size)

        # while channel.recv_ready():
        #     LOG.debug('==========')
        #     out += channel.recv(buff_size)
        # while channel.recv_stderr_ready():
        #     LOG.debug('++++++++++')
        #     err += channel.recv_stderr(buff_size)

        LOG.debug('[%s] Channel closed? %r', self.alias, channel.closed)
        while not channel.closed:
            # LOG.debug('...')
            # if channel.recv_ready():
            #     _out = channel.recv(buff_size)
            #     LOG.debug('OUT: %r', _out)
            #     out += _out
            # else:
            #     LOG.debug('not recv ready')
            # if channel.recv_stderr_ready():
            #     _err = channel.recv_stderr(buff_size)
            #     LOG.debug('ERR: %r', _err)
            #     err += _err
            # else:
            #     LOG.debug('not recv stderr ready')
            if channel.recv_ready():
                out += channel.recv(buff_size)
            if channel.recv_stderr_ready():
                err += channel.recv_stderr(buff_size)

        LOG.debug('[%s] channel.recv_ready()? %r', self.alias, channel.recv_ready())
        while channel.recv_ready():
            out += channel.recv(buff_size)
        LOG.debug('[%s] channel.recv_stderr_ready()? %r', self.alias, channel.recv_stderr_ready())
        while channel.recv_stderr_ready():
            err += channel.recv_stderr(buff_size)

        code = channel.recv_exit_status()

        # log last 5 lines for debug
        last_line_number = 5
        last_lines = out.splitlines()[-last_line_number:]
        last_line_number = len(last_lines)
        for index, line in enumerate(last_lines):
            LOG.debug('[%s] OUT[-%d]: %s', self.alias, (last_line_number - index), line)

        # LOG.debug('[%s] ExitCode: %s', self.alias, code)
        # LOG.debug('[%s] StdOut: %s', self.alias, out)
        # LOG.debug('[%s] StdErr: %s', self.alias, err)
        return code, out, err

    def upload(self, local_path, remote_path):
        dirname = os.path.dirname(remote_path)
        if dirname not in self.created_dirs:
            self.execute('mkdir -p ' + dirname)
            self.created_dirs.append(dirname)

        try:
            LOG.debug('[%s] Upload %s to %s (SIZE: %.2fMB)', self.alias, local_path, remote_path,
                      os.path.getsize(local_path) / 1024 / 1024)
            self.sftp.put(local_path, remote_path)
        except Exception as error:
            LOG.exception('[%s] %s', self.alias, error)

    def download(self, remote_path, local_path):
        try:
            fs.mkdir_p(os.path.dirname(local_path))
            LOG.debug('[%s] Download %s to %s', self.alias, remote_path, local_path)
            self.sftp.get(remote_path, local_path)
        except Exception as error:
            LOG.exception('[%s] %s', self.alias, error)

    def close(self):
        LOG.info('[%s] closing...', self.alias)
        self.transport.close()


class SSHChain(object):

    def __init__(self):
        self.connections = []
        self.transport = None

    def use(self, connection_index=-1):
        return self.connections[connection_index]

    def append(self, host, port=22, username='root', password=None, key_filename=None):
        ssh = SSHConnection(host, port, username, password, key_filename)
        if not self.connections:
            self.connections.append(ssh.connect())
        else:
            last_connection = self.connections[-1]
            self.connections.append(ssh.connect(last_connection))

    def close(self):
        for client in self.connections:
            client.close()
        LOG.info(' OVER '.center(50, '~'))

    def execute(self, *args, **kwargs):
        return self.use().execute(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
