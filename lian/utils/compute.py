# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

from decimal import Decimal


def change_rate(price_after, price_before, percent=True):
    change = Decimal(price_after) - Decimal(price_before)
    if percent:
        change *= 100
    return change / Decimal(price_before)


def decimal_align(a, places=8):
    return Decimal(a).quantize(Decimal('0.%s1' % ('0' * (places - 1))))


def __test():
    print(change_rate('1.222222222222', '2.333333333333'))
    print(repr(decimal_align('0.733293923238')))


if __name__ == '__main__':
    __test()
