# -*- coding: utf-8 -*-


def int2ordinal(num):
    assert isinstance(num, int) and num > 0
    return '%d%s' % (num, {1: 'st', 2: 'nd', 3: 'rd'}.get(num % 10, 'th'))


def _test_int2ordinal():
    for i in range(1, 100):
        print(int2ordinal(i))


def _test():
    for name, method in globals().items():
        if name.startswith('_test_') and callable(method):
            print('\n' + (' RUN: %s ' % name).center(100, '-') + '\n')
            method()


if __name__ == '__main__':
    _test()
