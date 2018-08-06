# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import datetime
import time

DEFAULT_DATE = '%Y/%m/%d'
DEFAULT_TIME = '%H:%M:%S'
DEFAULT_DATETIME = DEFAULT_DATE + ' ' + DEFAULT_TIME


def time_str(dt=None, fmt=None):
    if not dt:
        dt = datetime.datetime.now()
    if not fmt:
        fmt = DEFAULT_DATETIME
    return dt.strftime(fmt)


def time_str_simplified(dt=None):
    return time_str(dt, fmt='%Y%m%d%H%M%S')


def get_time_ten_min_align(t=None):
    if not t:
        t = datetime.datetime.now()
    assert isinstance(t, datetime.datetime) and 0 < t.minute < 60
    # 将分钟定位到 00 10 20 30 40 50
    for i in range(0, 60, 10):
        if t.minute > i + 10:
            continue
        return t.replace(minute=i, second=0, microsecond=0)


def get_timestamp(keep_int=False):
    ts = time.time()
    if keep_int:
        ts = int(ts)
    return ts


def get_timestamp_ms(keep_int=False):
    """ millisecond: ms """
    ts = get_timestamp() * 1000
    if keep_int:
        ts = int(ts)
    return ts


def get_timestamp_us(keep_int=False):
    """ microsecond: μs """
    ts = get_timestamp() * 1000000
    if keep_int:
        ts = int(ts)
    return ts


def dt2stamp(dt):
    return time.mktime(dt.timetuple()) + dt.microsecond / 1000000


def stamp2dt(stamp):
    return datetime.datetime.fromtimestamp(stamp)


def stamp2str(stamp, fmt=None):
    return time_str(stamp2dt(stamp), fmt=fmt)


def __test():
    print(get_time_ten_min_align())


if __name__ == '__main__':
    __test()
