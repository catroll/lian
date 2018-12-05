#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import re

import settings
from lian import ssh_deploy
from lian.ssh import SSHChain

PROG = 'Project Deploy Tool'
logging.basicConfig(level=settings.LOG_LEVEL, format=settings.LOG_FORMAT)
LOG = logging.getLogger(__name__)

with open(os.path.join(settings.CURRENT_DIR, 'connection.json')) as f:
    HOSTS = json.load(f)


def build_chain(hosts):
    if not hosts:
        LOG.error('Build connections chain error, no hosts')
        exit()

    chain = SSHChain()
    for host in hosts:
        chain.append(**HOSTS[host])
        chain.execute('uname -a')  # test
        LOG.info('-' * 50)
    return chain


def cli():
    def check_hosts(value):
        hosts = value.split('/')
        for host in hosts:
            if host not in HOSTS:
                raise argparse.ArgumentTypeError('host %r not configurated' % host)
        return hosts

    def check_local(value):
        if value.startswith('/'):
            local = value
        elif value.startswith('~/'):
            local = os.path.expanduser(value)
        else:
            local = os.path.join(os.getcwd(), value)
        if not os.path.isdir(local):
            raise argparse.ArgumentTypeError('%r is an invalid local directory' % value)
        return local

    def check_remote(value):
        if not value.startswith('/'):
            raise argparse.ArgumentTypeError('%r is an invalid remote directory' % value)
        return value

    def check_action(value):
        if value not in ssh_deploy.ACTIONS:
            raise argparse.ArgumentTypeError('%r is an invalid action' % value)
        return value

    def check_ignore(value):
        dirs = [i.strip() for i in value.split(',')]
        return dirs

    import argparse

    parser = argparse.ArgumentParser(description=PROG)
    parser.add_argument('hosts', type=check_hosts, help='eg: jump/hk.jump/test-gr5h')
    parser.add_argument('local', type=check_local, help='abspath, relpath, or startswith ~/')
    parser.add_argument('remote', type=check_remote, help='abspath')
    parser.add_argument('-a', '--action', type=check_action, choices=ssh_deploy.ACTIONS, help='default: check')  # default='check'
    parser.add_argument('--ignore-dirs', type=check_ignore)
    parser.add_argument('--ignore-files', type=check_ignore)
    parser.add_argument('-i', '--ignore', type=str)
    parser.add_argument('-u', type=str, dest='upload_files', nargs='*')
    parser.add_argument('-d', type=str, dest='download_files', nargs='*')
    parser.add_argument('-t', dest='test', action='store_true', help='test (show arguments)')

    args = parser.parse_args()

    if args.ignore_dirs is None:
        args.ignore_dirs = []
    default_dirs = '.git', '.svn', '.vscode', '.idea'
    args.ignore_dirs.extend([i for i in default_dirs if i not in args.ignore_dirs])

    if args.ignore_files is None:
        args.ignore_files = []

    if not args.action:
        args.action = 'sync' if args.upload_files or args.download_files else 'check'

    # print(args)
    return args


def main():
    """
    ssh_deploy.main(chain, local_path, remote_path, action='check',
                    files_upload=None, ignore_patterns=None, files_download=None,
                    *md5sum_args, **md5sum_kwargs):
    """
    args = cli()

    title = ' [%s] ***' % PROG
    print('*' * (80 - len(title)) + title)
    print('  Remote Hosts : %s' % (' -> '.join(args.hosts)))
    print('    Local Path : %s' % args.local)
    print('   Remote Path : %s' % args.remote)
    print('  Upload Files : %s' % args.upload_files)
    print('Download Files : %s' % args.download_files)
    print('        Action : %s' % args.action)
    print(' Ignored  Dirs : %s' % args.ignore_dirs)
    print(' Ignored Files : %s' % args.ignore_files)
    print('*' * 80)

    if args.test:
        return

    if args.ignore_dirs:
        not_match_dir = '(.*/)?(%s)/.*' % ('|'.join([re.escape(i) for i in args.ignore_dirs]))
    else:
        not_match_dir = None

    if args.ignore_files:
        not_match_file = '.*/(%s)' % ('|'.join([re.escape(i) for i in args.ignore_files]))
    else:
        not_match_file = None

    not_match = '(%s)' % ('|'.join(['(%s)' % i for i in [not_match_dir, not_match_file, args.ignore] if i]))
    print('Ignore: %r' % not_match)

    chain = build_chain(args.hosts)
    try:
        ignore_patterns = []
        ssh_deploy.main(chain, args.local, args.remote, action=args.action,
                        files_upload=args.upload_files, ignore_patterns=ignore_patterns,
                        files_download=args.download_files,
                        not_match=not_match)
    except Exception as error:
        LOG.exception('Uncaught Exception: %s', error)
    finally:
        chain.close()


if __name__ == '__main__':
    main()
