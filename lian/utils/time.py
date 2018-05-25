# -*- coding: utf-8 -*-

import datetime

DEFAULT_DATE = '%Y/%m/%d'
DEFAULT_TIME = '%H/%M/%S'
DEFAULT_DATETIME = DEFAULT_DATE + ' ' + DEFAULT_TIME


def time_str(dt, fmt=DEFAULT_DATETIME):
    if not dt:
        dt = datetime.datetime.now()
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


def __test():
    print(get_time_ten_min_align())


if __name__ == '__main__':
    __test()
