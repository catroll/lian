# -*- coding: utf-8 -*-

import logging
import os
import re
import time
from collections import OrderedDict

LOG = logging.getLogger(__name__)


def md5sum_command(directory='.', find_type='f', match='', not_match=''):
    return ' '.join([i for i in [
        'find', directory,
        ('-type %s' % find_type) if find_type else '',

        '-regextype posix-extended' if match or not_match else '',
        ('-regex %s' % match) if match else '',
        ('! -regex "%s"' % not_match) if not_match else '',

        """-print0 | xargs -0 md5sum | awk '{printf "%-50s %s\\n", $2, $1}' | sort"""
    ] if i])


def check_sum(chain, local_path, remote_path, *md5sum_args, **md5sum_kwargs):
    title = re.sub('[^a-zA-Z0-9]', '-', local_path) + '.' + time.strftime('%Y%m%d-%H%I%S')
    cmd_md5sum = md5sum_command(*md5sum_args, **md5sum_kwargs)

    # ---------- get md5sum ----------

    # locally
    command = 'cd ' + local_path + '; ' + cmd_md5sum
    LOG.info('local command: %s', command)
    content = os.popen(command).read()
    with open('/tmp/%s.a.txt' % title, 'w') as _file:
        _file.write(content)
    local_sums = OrderedDict((_file, _sum) for _file, _sum in [line.split() for line in content.splitlines()])

    # remotely
    command = 'cd ' + remote_path + '; ' + cmd_md5sum
    LOG.info('remote command: %s', command)
    code, out, err = chain.execute('cd ' + remote_path + '; ' + cmd_md5sum, buff_size=1024000)
    out = out.decode('utf-8')
    with open('/tmp/%s.b.txt' % title, 'w') as _file:
        _file.write(out)
    remote_sums = OrderedDict((_file, _sum) for _file, _sum in [line.split() for line in out.splitlines()])

    # ---------- compare result ----------

    LOG.info('*' * 50)
    LOG.info('')

    is_synced = True
    for _file in local_sums:
        if _file not in remote_sums:
            is_synced = False
            LOG.info(u'🐈 [LOCAL] ' + _file)
            continue
        if local_sums[_file] != remote_sums[_file]:
            is_synced = False
            LOG.info(u'🐍 [DIFF] ' + _file)
            continue
        # LOG.info('[SAME] ' + _file + ' ignore it')

    for _file in remote_sums:
        if _file not in local_sums:
            is_synced = False
            LOG.info(u'🐦 [REMOTE] ' + _file)

    if is_synced:
        LOG.info(u'㊗️ ㊗️ ㊗️ Perfect!!! ㊗️ ㊗️ ㊗️'.center(44))

    LOG.info('')
    LOG.info('*' * 50)


def sftp_download(chain, files_will_transferred):
    for remote_path, local_path in files_will_transferred:
        try:
            chain.use().download(remote_path, local_path)
        except Exception as error:
            LOG.warning(error)


def download_files(chain, local_path, remote_path, files=None):
    # download specified files
    if not files:
        LOG.debug('Download, but no file specified, over!')
        return
    move_tasks = [(os.path.join(remote_path, path), os.path.join(local_path, path)) for path in files]
    sftp_download(chain, move_tasks)


def sftp_upload(chain, files_will_transferred):
    """ SFTP upload
    Args:
        chain: object of SSHChain
        files_will_transferred: list[tuple]
    """
    LOG.info(files_will_transferred)
    for local_path, remote_path in files_will_transferred:
        chain.use().upload(local_path, remote_path)


def upload_files(chain, local_path, remote_path, files=None, ignore_patterns=None):
    """Upload local files or directory, can ignore some files by pattern

    Args:
        chain:
        local_path:
        remote_path:
        files:
        ignore_patterns:
    """
    files = files or []
    ignore_patterns = ignore_patterns or []

    re_ignore = re.compile('(%s)' % (')|('.join(ignore_patterns))) if ignore_patterns else ''
    move_tasks = []

    for path in files:
        fullpath = os.path.join(local_path, path)
        if not os.path.exists(fullpath):
            LOG.error('The file need uploaded not found: %s', fullpath)
            exit()

        if os.path.isfile(fullpath):
            move_tasks.append((fullpath, os.path.join(remote_path, path)))
            continue

        assert os.path.isdir(fullpath)
        for root, dirs, _files in os.walk(fullpath):
            for _file in _files:
                _fullpath = os.path.join(root, _file)
                if re_ignore and re_ignore.search(_fullpath):
                    continue
                relpath = os.path.relpath(_fullpath, local_path)
                move_tasks.append((_fullpath, os.path.join(remote_path, relpath)))

    sftp_upload(chain, move_tasks)


def file_sync(chain, local_path, remote_path,
              files_upload=None, ignore_patterns=None,  # upload arguments
              files_download=None):  # download arguments

    if files_download:
        download_files(chain, local_path, remote_path, files_download)

    if files_upload:
        upload_files(chain, local_path, remote_path, files_upload, ignore_patterns)


ACTIONS = 'check', 'sync', 'all',


def main(chain, local_path, remote_path, action='check',
         files_upload=None, ignore_patterns=None, files_download=None,
         *md5sum_args, **md5sum_kwargs):
    """
    Args:
        chain: object of SSHChain
        local_path: str, absolute path
        remote_path: str, absolute path
        action: str
        files_upload: list of files to upload
        ignore_patterns
        files_download: list of files to download
        md5sum_args:
        md5sum_kwargs: like: directory='.', find_type='f', match='', not_match=''
    """
    if action not in ACTIONS:
        return

    def _file_sync():
        file_sync(chain, local_path, remote_path, files_upload, ignore_patterns, files_download)

    def _check_sum():
        check_sum(chain, local_path, remote_path, *md5sum_args, **md5sum_kwargs)

    if action == 'sync':
        _file_sync()
        return

    if action == 'check':
        _check_sum()
        return

    _file_sync()
    _check_sum()
