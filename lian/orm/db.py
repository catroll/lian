# -*- coding: utf-8 -*-

"""
DB Connection

host=None, user=None, password="",
database=None, port=0, unix_socket=None,
charset='', sql_mode=None,
read_default_file=None, conv=None, use_unicode=None,
client_flag=0, cursorclass=Cursor, init_command=None,
connect_timeout=10, ssl=None, read_default_group=None,
compress=None, named_pipe=None,
autocommit=False, db=None, passwd=None, local_infile=False,
max_allowed_packet=16*1024*1024, defer_connect=False,
auth_plugin_map=None, read_timeout=None, write_timeout=None,
bind_address=None, binary_prefix=False, program_name=None,
server_public_key=None
"""

from __future__ import absolute_import, print_function

import copy
import logging
import re
import threading
import time

import pymysql
from six.moves.queue import Queue

from lian.orm.sql import escaped_str, escaped_var, make_tree
from lian.utils.naming import camel2underline

DEFAULT_DB = 'default'
DEFAULT_CONNECTIONS = 5
DEFAULT_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'MaxConnections': DEFAULT_CONNECTIONS,
}
LOG = logging.getLogger(__name__)


class PooledConnection(object):
    def __init__(self, pool, db, connection):
        self._connection = connection
        self._db = db
        self._pool = pool

    def close(self):
        if self._connection is not None:
            self._pool.return_connection(self._connection, self._db)
            self._connection = None

    def __getattr__(self, name):
        # All other members are the same.
        return getattr(self._connection, name)

    def __del__(self):
        self.close()


def init(config):
    ConnectionPool.init(config)


class ConnectionPool(object):
    _instance_lock = threading.Lock()
    _config = {}
    _queues = {}
    _logger = None

    @classmethod
    def init(cls, config):
        if cls.is_inited():
            raise Exception('ConnectionPool inited already')

        assert isinstance(config, dict)
        for db, db_config in config.items():
            assert isinstance(db, str)
            assert isinstance(db_config, dict)
            db_config.update({k: copy.deepcopy(v) for k, v in DEFAULT_CONFIG.items() if k not in db_config})
            if 'database' not in db_config and 'db' not in db_config:
                db_config['database'] = db

        if DEFAULT_DB not in config:
            first_db = next(iter(config.keys()))
            LOG.info('The database config has not DEFAULT_DB, choose %s as default database', first_db)
            config[DEFAULT_DB] = config[first_db]

        cls._config = config

    @classmethod
    def is_inited(cls):
        return cls._config != {}

    @classmethod
    def ensure_inited(cls):
        if not cls.is_inited():
            raise Exception('ConnectionPool need initialize')

    @classmethod
    def set_logger(cls, logger):
        cls._logger = logger

    @classmethod
    def get_config(cls, db=DEFAULT_DB):
        return cls._config[db]

    def __init__(self):
        self.ensure_inited()
        for db in self._config:
            db_config = self._config[db]
            self.logger = db_config.pop('Logger', self._logger or LOG)
            assert isinstance(self.logger, logging.Logger), 'param logger of db config error: %r' % self.logger
            _max_connections = db_config.pop('MaxConnections', DEFAULT_CONNECTIONS)
            self._queues[db] = Queue(_max_connections)  # create the queue
            for _ in range(_max_connections):
                conn = self.get_conn(db)
                self._queues[db].put(conn)

    @classmethod
    def instance(cls):
        cls.ensure_inited()
        if not hasattr(cls, '_instance'):
            with cls._instance_lock:
                if not hasattr(cls, '_instance'):
                    cls._instance = cls()
        return cls._instance

    def get_conn(self, db=DEFAULT_DB):
        self.ensure_inited()

        _config = copy.deepcopy(self._config[db])
        _config['passwd'] = '*' * 6
        self.logger.debug('connecting database: %r', _config)

        while True:
            try:
                conn = pymysql.connect(**self._config[db])
                conn.ping()
                break
            except Exception as e:
                self.logger.exception('connect failed: %s, retrying...', e)
                time.sleep(1)
        self.logger.info('database connected')
        return conn

    def connection(self, db=DEFAULT_DB):
        self.ensure_inited()
        conn = self._queues[db].get()
        try:
            conn.ping()
        except Exception as e:
            self.logger.exception(e)
            conn = self.get_conn(db)
        return PooledConnection(self, db, conn)

    def return_connection(self, conn, db=DEFAULT_DB):
        self.ensure_inited()
        self._queues[db].put(conn)


def _execute(sql, need_return=False, auto_commit=False, db=DEFAULT_DB):
    """Execute SQL

    :param sql:
    :param need_return:
    :param auto_commit:
    :return:
    """
    cur = None
    pool = ConnectionPool.instance()
    conn = pool.connection(db)
    started_at = time.time()
    try:
        cur = conn.cursor()
        rowcount = cur.execute(sql)
        if auto_commit:
            conn.commit()

        if need_return:
            result = {'rows': cur.fetchall()}
        else:
            result = {'rows': None}

        result.update({
            'conn': conn,
            'rowcount': rowcount,
            'description': cur.description,
            'lastrowid': cur.lastrowid,
        })
        # pool.logger.debug(result)
        return result
    except Exception as e:
        pool.logger.exception(e)
    finally:
        time_cost = time.time() - started_at
        if time_cost > 100:
            pool.logger.warning('Slow SQL: %s, cost: %f', sql, time_cost)
        else:
            pool.logger.debug('SQL: %s', sql)
        if cur:
            cur.close()


def execute(sql, auto_commit=False, db=DEFAULT_DB):
    return _execute(sql, need_return=False, auto_commit=auto_commit, db=db)


def query(sql, auto_commit=True, db=DEFAULT_DB):
    return _execute(sql, need_return=True, auto_commit=auto_commit, db=db)


class ObjectNotFound(Exception):
    pass


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
        _re_func_result = RE_FUNC.search(field)
        if _re_func_result:
            func, field = _re_func_result.groups()
            return '%s(`%s`)' % (func, escaped_str(field))
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


class BASE(object):
    __database__ = DEFAULT_DB
    __table__ = ''
    __pk__ = 'id'
    __fields__ = tuple()

    @property
    def table_name(self):
        if self.__class__ is BASE:
            raise NotImplementedError
        if self.__table__:
            return self.__table__
        return camel2underline(self.__class__.__name__)

    @property
    def database_name(self):
        if self.__database__ == DEFAULT_DB:
            config = ConnectionPool.get_config(db=DEFAULT_DB)
            name = config.get('database', None) or config.get('db', None)
            if name is None:
                raise Exception('model %s has not specified database!' % self.__class__.__name__)
            return name
        return self.__database__

    @property
    def full_table_name(self):
        return '%s.%s' % (self.database_name, self.table_name)

    @property
    def sql_table_name(self):
        return '`%s`.`%s`' % (self.database_name, self.table_name)

    def get(self, pk, key=None):
        if not key:
            key = self.__pk__
        rows = self.select(conditions={key: pk})
        if not rows:
            raise ObjectNotFound('%s #%s' % (self.full_table_name, pk))
        return rows[0]

    def _select_sql(self, fields=None, conditions=None, limit=None, offset=None, order_by=None, group_by=None,
                    raw_conditions=None):

        if not fields:
            fields = self.__fields__ or None

        fields_str = _fields_sql(fields, select_mode=True) or '*'
        conditions_sql = raw_conditions or make_tree(conditions)
        sql = 'SELECT %s FROM %s WHERE %s' % (fields_str, self.sql_table_name, conditions_sql)

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

    def select(self, fields=None, conditions=None, limit=None, offset=None, order_by=None, group_by=None,
               raw_sql=None, raw_conditions=None):
        sql = raw_sql or self._select_sql(fields, conditions, limit, offset, order_by, group_by, raw_conditions)
        result = query(sql, db=self.__database__)
        return result['rows'] if result else []

    def insert(self, values, fields=None, update=None):
        assert isinstance(values, (dict, list, tuple))
        if isinstance(values, dict):
            fields = list(values.keys())
            values = [values[i] for i in fields]
        else:
            if not fields:
                fields = self.__fields__
            assert len(values) == len(fields)
        values_str = ', '.join([escaped_var(val) for val in values])
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.sql_table_name, _fields_sql(fields), values_str)

        if update:
            sql += ' ON DUPLICATE KEY UPDATE %s' % _set_sql(update)

        result = execute(sql, auto_commit=True, db=self.__database__)
        return self.get(result['lastrowid']) if result else None

    def update(self, values, conditions=None):
        sql = 'UPDATE %s SET %s WHERE %s' % (self.sql_table_name, _set_sql(values), make_tree(conditions))
        result = execute(sql, auto_commit=True, db=self.__database__)
        return result['rowcount']  # 影响行数

    def count(self, conditions=None):
        sql = 'SELECT COUNT(1) FROM %s WHERE %s' % (self.sql_table_name, make_tree(conditions))
        result = query(sql, db=self.__database__)
        return result['rows'][0]['COUNT(1)'] if result else 0
