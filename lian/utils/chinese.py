# -*- encoding: utf-8 -*-

from __future__ import unicode_literals

import re

import six


class NotIntegerError(Exception):
    pass


class OutOfRangeError(Exception):
    pass


_MAPPING = '零一二三四五六七八九'
_P0 = '', '十', '百', '千'
_S4, _S8, _S16 = 10 ** 4, 10 ** 8, 10 ** 16
_MIN, _MAX = 0, 9999999999999999


def num2lst(num):
    lst = []
    while num >= 10:
        lst.append(num % 10)
        num = int(num / 10)
    lst.append(num)
    return lst


def to_chinese(num):
    def _to4(_num):
        if _num < 10:
            return _MAPPING[_num]

        lst = num2lst(_num)
        c = len(lst)  # 位数
        result = ''
        for idx, val in enumerate(lst):
            if val == 0:
                continue
            result += _P0[idx] + _MAPPING[val] + ('零' if idx < c - 1 and lst[idx + 1] == 0 else '')
        result = result[::-1]
        # return result.replace('一十', '十')
        return result

    def _to8(_num):
        if _num < _S4:
            return _to4(_num)
        mod = _S4
        high, low = int(_num / mod), _num % mod
        if low == 0:
            return _to4(high) + '万'
        if low < int(_S4 / 10):
            return _to4(high) + '万零' + _to4(low)
        return _to4(high) + '万' + _to4(low)

    def _to16(_num):
        to8 = _to8
        mod = _S8
        high, low = int(_num / mod), _num % mod
        if low == 0:
            return to8(high) + '亿'
        if low < int(_S8 / 10):
            return to8(high) + '亿零' + to8(low)
        return to8(high) + '亿' + to8(low)

    if not isinstance(num, six.integer_types):
        raise NotIntegerError('%s is not a integer.' % num)

    if num < _MIN or num > _MAX:
        raise OutOfRangeError('%d out of range[%d, %d)' % (num, _MIN, _MAX))

    if num < _S4:
        return _to4(num)
    if num < _S8:
        return _to8(num)
    return _to16(num)


CHS_UNITS = {
    '十': 1, '百': 2, '千': 3, '万': 4, '亿': 8,
    '拾': 1, '佰': 2, '仟': 3, '萬': 4, '億': 8,
}
CHS_NUMBERS = {
    '零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '〇': 0, '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9,
    '貮': 2, '两': 2,
}
RE_LITER_NUMBER = re.compile(('([%(numbers)s](千|仟))?'
                              '([%(numbers)s](百|佰)|(零|〇))?'
                              '([%(numbers)s]?(十|拾)|(零|〇))?'
                              '([%(numbers)s])?') % {'numbers': ''.join(CHS_NUMBERS.keys())})


def to_int(num_str, rest_mode=True):
    if not num_str:
        return 0

    for chs_sep in '億亿萬万':
        if chs_sep in num_str:
            assert num_str.count(chs_sep) == 1, 'this chinese number string seem wrong: %s' % num_str
            part_a, part_b = num_str.split(chs_sep)
            part_a_num = to_int(part_a, rest_mode=False) * (10 ** CHS_UNITS[chs_sep])
            part_b_num = to_int(part_b)
            return part_a_num + part_b_num

    # x千x百x十x
    assert RE_LITER_NUMBER.match(num_str), 'this chinese number string seem wrong: %s' % num_str

    number = part_number = 0
    digits = 0  # 位数
    for c in num_str:
        if c in '零〇':
            digits = 0
        elif c in CHS_NUMBERS:
            part_number = CHS_NUMBERS[c]
        elif c in '拾十佰百仟千':
            digits = CHS_UNITS[c]
            if part_number == 0:
                if number == 0:
                    number = 10 ** digits
                    continue
                else:
                    part_number = 1
            number += part_number * (10 ** digits)
            part_number = 0
        # print('[DEBUG] %s: number %r, part_number %r, digits %s' % (c, number, part_number, digits))

    if rest_mode:
        number += part_number * ((10**(digits-1)) if digits > 1 else 1)
    else:
        number += part_number

    return number


def __test():
    demo_data = {
        '二': 2,
        '二十': 20,
        '十二': 12,
        '一十二': 12,
        '二百二': 220,
        '二百二十': 220,
        '二百零二': 202,
        '二百二十二': 222,
        '五百二十': 520,
        '三千零一十五': 3015,
        '三千零十五': 3015,
        '一亿': 100000000,
        '一亿零一': 100000001,
        '一万亿': 1000000000000,
        '壹佰贰拾叁亿壹仟伍佰壹拾伍万零叁佰零贰': 12315150302,
    }
    for chs, num in demo_data.items():
        converted = to_int(chs)
        assert converted == num, 'converted %r != %r' % (converted, num)
        print('[OK] %s => %r' % (chs, num))

    demo_data = (2, 10, 11, 12, 22, 502, 520, 1000, 1002, 1010, 1050, 1500, 1503,
                 10000, 10001, 15201, 99999, 100000,
                 9999999999999999, 1056345102012015)
    for i in demo_data:
        chs = to_chinese(i)
        reconverted = to_int(chs)
        assert reconverted == i, '%r reconverted: %r != %r' % (chs, reconverted, i)
        print('[OK] %r => %r' % (i, chs))


if __name__ == '__main__':
    __test()
