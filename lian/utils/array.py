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


if __name__ == '__main__':
    __test()
