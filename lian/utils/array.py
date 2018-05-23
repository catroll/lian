# -*- coding: utf-8 -*-


def permutation(array):
    """排列组合

    [0, 1, 2] => [[0, 1], [0, 2], [1, 2]]
    """
    perm_list = []
    for i in range(0, len(array)):
        for j in range(i + 1, len(array)):
            perm_list.append([array[i], array[j]])
    return perm_list

def __test():
    permutation([1, 2, 3, 4])

    print(gen_id())
    print(get_time_ten_min_align())
    print('{0:g}'.format(float('0.00110000')))
    print(Decimal('{0:g}'.format(float('0.00110000'))))

    amount = '98.73329392323'
    amount = Decimal(amount).quantize(Decimal('{0:g}'.format(float('0.01'))))
    print(amount)


if __name__ == '__main__':
    __test()
