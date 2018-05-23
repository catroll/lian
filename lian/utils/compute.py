# -*- coding: utf-8 -*-

# Copyright 2009 SendCloud
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

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
