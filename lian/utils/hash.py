# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import hashlib
import time
import uuid
from hashlib import md5

from lian.utils.time import time_str_simplified


def gen_md5(s):
    if isinstance(s, str):
        s = s.encode('utf-8')
    assert isinstance(s, bytes), 's must be bytes type.'
    m = md5()
    m.update(s)
    return m.hexdigest()


def checksum(filepath, hash_type='md5'):
    """

    Args:
        filepath:
        hash_type: str, support 'md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512'

    Returns:
        str
    """
    _size = 4096

    m = getattr(hashlib, hash_type)()
    with open(filepath, 'rb') as fp:
        # data = fp.read(_size)
        # while data:
        #     m.update(data)
        #     data = fp.read(_size)
        for chunk in iter(lambda: fp.read(_size), b''):
            m.update(chunk)
    return m.hexdigest()


def gen_id():
    s = str(uuid.uuid3(uuid.uuid1(), gen_md5(time.ctime().encode('ascii')))).replace('-', '')
    return '%s_%s' % (time_str_simplified(), s)


def __test():
    for hash_type in ('md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512'):
        print('%6s' % hash_type, checksum('/etc/hosts', hash_type))
    print()

    cases = 1, 'abc', b'abc', '国际共产主义', b'AlphaGo',
    for case in cases:
        try:
            print(gen_md5(case))
        except Exception as error:
            print(repr(error))

    print(gen_id())


if __name__ == '__main__':
    __test()
