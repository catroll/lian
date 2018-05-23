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
