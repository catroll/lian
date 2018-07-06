# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function


def int2ordinal(num):
    assert isinstance(num, int) and num > 0
    return '%d%s' % (num, {1: 'st', 2: 'nd', 3: 'rd'}.get(num % 10, 'th'))


def __test_int2ordinal():
    for i in range(1, 100):
        print(int2ordinal(i))


def __test():
    for name, method in globals().items():
        if name.startswith('__test_') and callable(method):
            print('\n' + (' RUN: %s ' % name).center(100, '-') + '\n')
            method()


if __name__ == '__main__':
    __test()
