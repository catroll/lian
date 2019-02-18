# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function


def combinations(*iterables):
    result = [[]]
    for iterable in iterables:
        result = [x+[y] for x in result for y in iterable]
    return result


def permutation(array):
    """排列组合

    [0, 1, 2] => [[0, 1], [0, 2], [1, 2]]
    """
    result = []
    for index, item_a in enumerate(array):
        for item_b in array[index + 1:]:
            result.append([item_a, item_b])
    return result


def __test():
    print(permutation([0, 1, 2]))


if __name__ == '__main__':
    __test()
