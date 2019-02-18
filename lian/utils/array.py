# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function


def combinations(*iterables):
    result = [[]]
    for iterable in iterables:
        result = [x+[y] for x in result for y in iterable]
    return result


def permutation(iterable, r=2):
    result = [[]]
    for _ in range(r):
        result = [x + [y] for x in result for y in iterable]
    return result


def permutation_unique(iterable, r=3):
    """组合

    [0, 1, 2] => [[0, 1], [0, 2], [1, 2]]
    """
    # result = []
    # for i, item_a in enumerate(iterable):
    #     for item_b in iterable[i + 1:]:
    #         result.append([item_a, item_b])
    right_index = len(iterable) - r + 1
    result = [[(i, index)] for index, i in enumerate(iterable)][:right_index]
    for i1 in range(r - 1):
        # _result = []
        # for x in result:
        #     print([i[0] for i in x])
        #     for i2, y in enumerate(iterable[x[-1][1] + 1:right_index + i1 + 1]):
        #         print('+', y)
        #         _result.append(x + [(y, x[-1][1] + 1 + i2)])
        # print('=' * 30)
        # result = _result
        result = [x + [(y, x[-1][1] + 1 + i2)] for x in result for i2, y in enumerate(iterable[x[-1][1] + 1:right_index + i1 + 1])]
    return [[j[0] for j in i] for i in result]


def __test():
    # x = permutation_unique('abcdefghijklmnopqrstuvwxyz', 3)
    import time
    started_at = time.time()
    x = permutation_unique(range(1, 6), 3)
    time_cost = (time.time() - started_at) * 1000
    x_str = '\n'.join([
        '  '.join([
            ('[%s]' % (', '.join(['%3s' % z for z in j])))
            for j in x[i:i+5]
        ])
        for i in range(0, len(x), 5)
    ])
    print('=' * 50)
    print('Cost %0.3f ms, result (length: %s):\n%s' % (time_cost, len(x), x_str))


if __name__ == '__main__':
    __test()
