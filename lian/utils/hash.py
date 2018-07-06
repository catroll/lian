# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

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


def gen_id():
    s = str(uuid.uuid3(uuid.uuid1(), gen_md5(time.ctime().encode('ascii')))).replace('-', '')
    return '%s_%s' % (time_str_simplified(), s)


def __test():
    cases = 1, 'abc', b'abc', '国际共产主义', b'AlphaGo',
    for case in cases:
        try:
            print(gen_md5(case))
        except Exception as error:
            print(repr(error))

    print(gen_id())


if __name__ == '__main__':
    __test()
