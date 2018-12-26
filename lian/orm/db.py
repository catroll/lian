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
        self.logger = pool.get_logger(db)

    def close(self):
        if self._connection is not None:
            if self._pool.is_connection_using(self._connection, self._db):
                self._pool.release(self._connection, self._db)
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
    _logger = None
    _default_db = DEFAULT_DB

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

        if cls._default_db not in config:
            cls._default_db = next(iter(config.keys()))
            LOG.info('The database config has not DEFAULT_DB, choose %s as default database', cls._default_db)

        cls._config = config

    @classmethod
    def is_inited(cls):
        return cls._config != {}

    @classmethod
    def ensure_inited(cls):
        if not cls.is_inited():
            raise Exception('ConnectionPool need initialize')

    @classmethod
    def real_db(cls, db=None):
        if not db or db == DEFAULT_DB:
            db = cls._default_db
        return db

    @classmethod
    def set_logger(cls, logger):
        cls._logger = logger

    @classmethod
    def get_config(cls, db=None):
        return cls._config[cls.real_db(db)]

    def get_logger(self, db=None):
        return self._loggers.get(self.real_db(db), self._logger)

    def get_queue(self, db=None):
        return self._queues[self.real_db(db)]

    def get_queue_using(self, db=None):
        return self._queues_using[self.real_db(db)]

    def __init__(self):
        self.ensure_inited()
        self.logger = self._logger or LOG

        self._queues = {}
        self._queues_using = {}
        self._loggers = {}

        for db in self._config:
            db_config = self.get_config(db)

            logger = db_config.pop('Logger', self.logger)
            assert isinstance(logger, logging.Logger), 'param logger of db config error: %r' % logger
            self._loggers[db] = logger

            _max_connections = db_config.pop('MaxConnections', DEFAULT_CONNECTIONS)
            assert isinstance(_max_connections, int) and _max_connections > 0

            self._queues[db] = Queue(_max_connections)  # create the queue
            self._queues_using[db] = []

            self.build_connection(db)  # build a connection

    def get_queue_status(self, db):
        q = self.get_queue(db)
        q_using = self.get_queue_using(db)
        return 'queue of %s size: %s + %s / %s' % (db, q.qsize(), len(q_using), q.maxsize)

    def is_connection_using(self, conn, db=None):
        if db is None:
            return any((self.is_connection_using(conn, db) for db in self._config))
        id_conn = id(conn)
        q_using = self.get_queue_using(db)
        self.logger.debug('is_connection_using(%s): %r in %r', db, id_conn, q_using)
        return id_conn in q_using

    def build_connection(self, db):
        q = self.get_queue(db)
        q_using = self.get_queue_using(db)
        if q.qsize() + len(q_using) >= q.maxsize:
            return

        self.logger.debug('build connection at %s', db)
        conn = self.connect(db)
        q.put(conn)
        self.logger.debug('after build connection, %s', self.get_queue_status(db))

    @classmethod
    def instance(cls):
        cls.ensure_inited()
        if not hasattr(cls, '_instance'):
            with cls._instance_lock:
                if not hasattr(cls, '_instance'):
                    cls._instance = cls()
        return cls._instance

    def connect(self, db=DEFAULT_DB):
        self.ensure_inited()

        _config = copy.deepcopy(self.get_config(db))
        _config['passwd'] = '*' * 6
        self.logger.debug('connecting database: %r', _config)

        while True:
            try:
                conn = pymysql.connect(**self.get_config(db))
                LOG.debug('ping...')
                conn.ping()
                break
            except Exception as e:
                self.logger.exception('connect failed: %s, retrying...', e)
                time.sleep(1)
        self.logger.info('database connected')
        return conn

    def acquire(self, db=DEFAULT_DB):
        self.ensure_inited()

        q = self.get_queue(db)
        q_using = self.get_queue_using(db)
        self.logger.debug('acquire connection at %s', db)
        if q.empty():
            self.build_connection(db)

        while True:
            conn = q.get()

            conn_error = False
            try:
                LOG.debug('ping...')
                conn.ping()
                break
            except Exception as e:
                self.logger.exception(e)
                conn_error = True

            if conn_error:
                self.logger.warning('Connection error, try to close it...')
                try:
                    conn.close()
                except Exception as conn_close_error:
                    self.logger.exception(conn_close_error)
                self.build_connection(db)

        q_using.append(id(conn))
        self.logger.debug('after acquire connection, %s', self.get_queue_status(db))
        return conn

    def release(self, conn, db=DEFAULT_DB):
        # import traceback
        # self.logger.debug('TraceBack:\n' + (''.join(traceback.format_stack())))

        self.ensure_inited()

        id_conn = id(conn)

        q = self.get_queue(db)
        q_using = self.get_queue_using(db)

        if not self.is_connection_using(conn, db):
            self.logger.warning('The connection #%d is not using, ignore release... (using connections: %r)', id_conn,
                                q_using)
            return

        self.logger.debug('release connection %d at %s', id_conn, db)
        q.put(conn)
        q_using.remove(id_conn)
        self.logger.debug('after release connection, %s', self.get_queue_status(db))


class ConnectionContext:
    def __init__(self, db):
        self.db = db
        self.connection = None  # pymysql.Connection

    def __enter__(self):
        pool = ConnectionPool.instance()
        self.connection = pool.acquire(self.db)
        return PooledConnection(pool, self.db, self.connection)

    def __exit__(self, *args):
        """args: type, value, trace"""
        pool = ConnectionPool.instance()
        if pool.is_connection_using(self.connection, self.db):
            pool.release(self.connection, self.db)


def _execute(sql, need_return=False, auto_commit=False, db=DEFAULT_DB):
    """Execute SQL

    :param sql:
    :param need_return:
    :param auto_commit:
    :return:
    """
    with ConnectionContext(db) as conn:
        conn.logger.debug('sql execute start...')
        cur = None
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
            # conn.logger.debug(result)
            return result
        except Exception as e:
            conn.logger.exception(e)
        finally:
            time_cost = time.time() - started_at
            if time_cost > 100:
                conn.logger.warning('Slow SQL: %s, cost: %f', sql, time_cost)
            else:
                conn.logger.debug('SQL: %r, cost: %f', sql, time_cost)
            if cur:
                conn.logger.debug('Close cursor...')
                cur.close()
            conn.logger.debug('sql execute over...')


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
        LOG.debug(field)
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

    @property
    def logger(self):
        pool = ConnectionPool.instance()
        return pool.get_logger(self.database_name)

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
        conditions_sql = raw_conditions or make_tree(conditions, self.logger)
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

    def insert(self, values, fields=None, update=None, replace_mode=False):
        assert isinstance(values, (dict, list, tuple))
        if isinstance(values, dict):
            fields = list(values.keys())
            values = [values[i] for i in fields]
        else:
            if not fields:
                fields = self.__fields__
            assert len(values) == len(fields)
        values_str = ', '.join([escaped_var(val) for val in values])

        if replace_mode:
            sql = 'REPLACE INTO %s (%s) VALUES (%s)' % (self.sql_table_name, _fields_sql(fields), values_str)
        else:
            sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.sql_table_name, _fields_sql(fields), values_str)
            if update:
                sql += ' ON DUPLICATE KEY UPDATE %s' % _set_sql(update)

        result = execute(sql, auto_commit=True, db=self.__database__)
        return self.get(result['lastrowid']) if result else None

    def update(self, values, conditions=None):
        sql = 'UPDATE %s SET %s WHERE %s' % (self.sql_table_name, _set_sql(values), make_tree(conditions, self.logger))
        result = execute(sql, auto_commit=True, db=self.__database__)
        return result['rowcount']  # 影响行数

    def count(self, conditions=None):
        sql = 'SELECT COUNT(1) FROM %s WHERE %s' % (self.sql_table_name, make_tree(conditions, self.logger))
        result = query(sql, db=self.__database__)
        return result['rows'][0]['COUNT(1)'] if result else 0
