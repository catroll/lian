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

"""
一个小轮子：轻量级 ORM

逻辑关系：[] 表示 OR；()，{} 表示 AND
运算符：支持 is null，like，比较运算等
其他：字段与值都经过转义，避免注入
"""

import re

import pymysql

SQL_OPS = [
    'like', 'endswith', 'startswith', 'ne', 'gt', 'lt', 'gte', 'lte', 'in', 'is_null'
]
RE_SQL_OP = re.compile('^([^ ]+)__(%s)$' % ('|'.join(SQL_OPS)))


def escaped_str(_str):
    assert isinstance(_str, str)
    return pymysql.converters.escape_string(_str)


def escaped_var(_var):
    return pymysql.converters.escape_item(_var, 'utf-8')


class SQLNode(object):
    def __init__(self, key, value):
        self.key_orig = key
        self.value = value

        if value is None:
            self.key = key
            self.exp = 'is_null'
        else:
            _re_result = RE_SQL_OP.findall(key)
            if _re_result and callable(getattr(self, '_exp_' + _re_result[0][1], None)):
                self.key, self.exp = _re_result[0]
            else:
                self.key = key
                self.exp = None

    @property
    def escaped_value(self):
        return escaped_var(self.value)

    @property
    def escaped_key(self):
        return escaped_str(self.key)

    @property
    def escaped_value_str(self):
        return escaped_str(self.value)

    def _exp_nq(self):
        return "`%s` != %s" % (self.escaped_key, self.escaped_value)

    def _exp_like(self):
        return "`%s` LIKE '%%%s%%'" % (self.escaped_key, self.escaped_value_str)

    def _exp_startswith(self):
        return "`%s` LIKE '%s%%'" % (self.escaped_key, self.escaped_value_str)

    def _exp_endswith(self):
        return "`%s` LIKE '%%%s'" % (self.escaped_key, self.escaped_value_str)

    def _exp_lt(self):
        return '`%s` < %s' % (self.escaped_key, self.escaped_value)

    def _exp_lte(self):
        return '`%s` <= %s' % (self.escaped_key, self.escaped_value)

    def _exp_gt(self):
        return '`%s` > %s' % (self.escaped_key, self.escaped_value)

    def _exp_gte(self):
        return '`%s` >= %s' % (self.escaped_key, self.escaped_value)

    def _exp_in(self):
        return '`%s` IN %s' % (self.escaped_key, self.escaped_value)

    def _exp_is_null(self):
        return '`%s` IS %sNULL' % (self.escaped_key, '' if self.value else 'NOT ')

    def _exp_between(self):
        assert isinstance(self.value, (list, tuple)) and len(self.value) == 2
        return '`%s` BETWEEN %s AND %s' % (self.escaped_key, escaped_var(self.value[0]), escaped_var(self.value[1]))

    def __str__(self):
        if self.exp:
            exp = getattr(self, '_exp_' + self.exp, None)
            if callable(exp):
                return exp()
        return '`%s` = %s' % (self.escaped_key, self.escaped_value)


class SQLNodeTree(object):
    def __init__(self, nodes, relation='AND', reverse=False):
        assert relation in ('AND', 'OR')
        self.relation = relation
        self.reverse = reverse
        self.nodes = nodes

    def __str__(self):
        if not self.nodes:
            return '1'
        return '(' + (' %s ' % self.relation).join(str(i) for i in self.nodes) + ')'


def make_tree(conditions):
    if not conditions:
        return SQLNodeTree([])
    assert isinstance(conditions, (tuple, list, dict))
    if isinstance(conditions, dict):
        nodes = [SQLNode(k, v) for k, v in conditions.items()]
        return SQLNodeTree(nodes)
    elif isinstance(conditions, (list, tuple)):
        reverse = conditions[0] == 'NOT'
        if reverse:
            conditions = conditions[1:]
        relation = 'AND' if isinstance(conditions, tuple) else 'OR'
        return SQLNodeTree([make_tree(i) for i in conditions], relation=relation, reverse=reverse)
    else:
        raise Exception('make_tree error: condition type must be tuple, list, or dict, got %s', type(conditions))


def __test():
    print(make_tree({'a': 1}))
    print(make_tree({'a': []}))
    print(make_tree({'a__in': []}))
    print(make_tree({'a__in': [], 'b': None}))
    print(make_tree(({'a__in': [], 'b': None}, ({'c': 1},), ())))
    print(make_tree([{'a__in': [], 'b': None}, ({'c': 1},), ()]))


if __name__ == '__main__':
    __test()
