# -*- coding: utf-8 -*-


from __future__ import absolute_import, print_function

import logging
import re

from cached_property import cached_property

from lian.orm.nodes import escaped_str, escaped_var, make_tree

LOG = logging.getLogger(__name__)
SET_OPS = 'ADD',
RE_SET_OP = re.compile('^(%s):(.+)$' % ('|'.join(SET_OPS)))


def _set_sql(values):
    def _set_v(key, value):
        _re_op_result = RE_SET_OP.search(key)
        if _re_op_result:
            op, new_k = _re_op_result.groups()
            new_k = escaped_str(new_k)
            if op == 'ADD':
                return '`%s` = `%s` + %s' % (new_k, new_k, escaped_var(value))
        return '`%s` = %s' % (escaped_str(key), escaped_var(value))

    return ', '.join([_set_v(k, v) for k, v in values.items()])


FUNCS = 'SUM', 'MAX', 'MIN',
RE_FUNC = re.compile('^(%s):(.+)$' % ('|'.join(FUNCS)))


def _fields_sql(fields, select_mode=False):
    def _sql_func(field):
        # LOG.debug(field)
        _re_func_result = RE_FUNC.search(field)
        if _re_func_result:
            func, field = _re_func_result.groups()
            return '%s(`%s`)' % (func, escaped_str(field))
        if field.startswith('*'):
            return 'DISTINCT `%s`' % field[1:]
        return '`%s`' % field

    def _inner(field):
        if select_mode:
            if isinstance(field, tuple):
                assert len(field) == 2 and isinstance(field[0], str) and isinstance(field[1], str)
                return '%s AS `%s`' % (_sql_func(field[0]), escaped_str(field[1]))
        return _sql_func(field)

    if isinstance(fields, (tuple, list)) and fields:
        fields_str = ', '.join([_inner(f) for f in fields])
        return fields_str

    return None


class SQL:
    def __init__(self, table, database=None, logger=None):
        self.database = database
        self.table = table
        self.logger = logger or LOG

    @cached_property
    def sql_table(self):
        if self.database:
            return '`%s`.`%s`' % (self.database, self.table)
        return '`%s`' % self.table

    def select(self, fields=None, conditions=None, limit=None, offset=None, order_by=None, group_by=None):
        fields_str = _fields_sql(fields, select_mode=True) or '*'
        conditions_sql = make_tree(conditions, self.logger)
        sql = 'SELECT %s FROM %s WHERE %s' % (fields_str, self.sql_table, conditions_sql)

        if isinstance(group_by, (tuple, list, str)) and group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            sql += ' GROUP BY ' + (', '.join(['`%s`' % field for field in group_by]))

        if isinstance(order_by, str):
            order_by = [order_by]
        elif isinstance(order_by, (tuple, list)):
            order_by = list(order_by)
        else:
            order_by = []
        _order_by_sql = []
        for i in order_by:
            assert isinstance(i, str) and i
            if i.startswith('-'):
                i = '`%s` DESC' % escaped_str(i[1:])
            else:
                i = '`%s`' % i
            _order_by_sql.append(i)
        if _order_by_sql:
            sql += ' ORDER BY %s' % (', '.join(_order_by_sql))

        if isinstance(limit, int):
            sql += ' LIMIT %d' % limit

        if isinstance(offset, int):
            sql += ' OFFSET %d' % offset

        return sql

    def insert(self, values, fields=None, mode='insert', update=None, conditions=None):
        assert isinstance(values, (dict, list, tuple))
        if isinstance(values, dict):
            fields = list(values.keys())
            values = [values[i] for i in fields]
        else:
            assert len(values) == len(fields)
        values_str = ', '.join([escaped_var(val) for val in values])

        if conditions:
            sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.sql_table, _fields_sql(fields), values_str)
            if update:
                sql += ' ON DUPLICATE KEY UPDATE %s' % _set_sql(update)
        elif mode == 'replace':
            sql = 'REPLACE INTO %s (%s) VALUES (%s)' % (self.sql_table, _fields_sql(fields), values_str)
        elif mode == 'insert-not-exists':
            if not conditions:
                raise Exception('insert(mode insert-not-exists): must has conditions param')
            conditions_sql = make_tree(conditions, self.logger)
            sql = 'INSERT INTO %s (%s) SELECT * FROM (SELECT %s) AS tmp WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s) LIMIT 1' % (
                self.sql_table, _fields_sql(fields), values_str, self.sql_table, conditions_sql)
        else:
            raise Exception('error insert mode: %s' % mode)

        return sql

    def update(self, values, conditions=None):
        conditions_sql = make_tree(conditions, self.logger)
        sql = 'UPDATE %s SET %s WHERE %s' % (self.sql_table, _set_sql(values), conditions_sql)
        return sql

    def count(self, conditions=None):
        conditions_sql = make_tree(conditions, self.logger)
        sql = 'SELECT COUNT(1) FROM %s WHERE %s' % (self.sql_table, conditions_sql)
        return sql

    def delete(self, conditions=None):
        conditions_sql = make_tree(conditions, self.logger)
        sql = 'DELETE FROM %s WHERE %s' % (self.sql_table, conditions_sql)
        return sql
