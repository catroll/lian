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
import threading
import time
import uuid

import pymysql
from six.moves.queue import Queue

from lian.orm import statement
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
        LOG.debug('ConnectionPool.init')
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
            LOG.debug('All databases: %r', list(config.keys()))
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
        self.logger.info('init')

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
        query_uuid = uuid.uuid1()
        conn.logger.debug('[%s] db %s: sql execute start...', query_uuid, db)
        conn.logger.debug('[%s] sql: %s', query_uuid, sql)
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
            # conn.logger.debug('[%s] %r', query_uuid, result)
            return result
        except Exception as e:
            conn.logger.exception('[%s] db %s: %s', query_uuid, db, e)
        finally:
            time_cost = time.time() - started_at
            if time_cost > 100:
                conn.logger.warning('[%s] slow sql, cost: %f', query_uuid, time_cost)
            else:
                conn.logger.debug('[%s] cost: %f', query_uuid, time_cost)
            if cur:
                conn.logger.debug('[%s] close cursor...', query_uuid)
                cur.close()
            conn.logger.debug('[%s] db %s: sql execute over...', query_uuid, db)


def execute(sql, auto_commit=False, db=DEFAULT_DB):
    return _execute(sql, need_return=False, auto_commit=auto_commit, db=db)


def query(sql, auto_commit=True, db=DEFAULT_DB):
    return _execute(sql, need_return=True, auto_commit=auto_commit, db=db)


class ObjectNotFound(Exception):
    pass


class BASE(object):
    __database__ = DEFAULT_DB
    __table__ = ''
    __pk__ = 'id'
    __fields__ = tuple()

    def __init__(self):
        self.sql = statement.SQL(self.table_name, database=self.database_name, logger=self.logger)

    @property
    def table_name(self):
        if self.__class__ is BASE:
            raise NotImplementedError
        if self.__table__:
            return self.__table__
        return camel2underline(self.__class__.__name__)

    @property
    def database_name(self):
        # 获取真实的数据库名称
        config = ConnectionPool.get_config(db=self.__database__)
        name = config.get('database', None) or config.get('db', None)
        if name is None:
            raise Exception('model %s has not specified database!' % self.__class__.__name__)
        return name

    @property
    def full_table_name(self):
        return '%s.%s' % (self.database_name, self.table_name)

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

    def select(self, fields=None, conditions=None, limit=None, offset=None, order_by=None, group_by=None, raw_sql=None):
        sql = raw_sql or self.sql.select(fields, conditions, limit, offset, order_by, group_by)
        result = query(sql, db=self.__database__)
        return result['rows'] if result else []

    def find(self, fields=None, conditions=None, offset=None, order_by=None, raw_sql=None):
        count = self.count(conditions=conditions)
        if count == 0:
            return None
        rows = self.select(fields, conditions, limit=1, offset=offset, order_by=order_by, raw_sql=raw_sql)
        return rows[0]

    def insert(self, values, fields=None, mode='insert',
               update=None,  # mode = update
               conditions=None,  # mode = insert-not-exists
               refetch=False):
        if not fields:
            fields = self.__fields__
        sql = self.sql.insert(values, fields=fields, mode=mode, update=update, conditions=conditions)
        result = execute(sql, auto_commit=True, db=self.__database__)
        if not result:
            self.logger.warning('insert return %s: %s', result, sql)
            return None
        return self.get(result['lastrowid']) if refetch else result['lastrowid']

    def insert_many(self, fields, values_list, update_fields=None):
        if not fields:
            fields = self.__fields__
        sql = self.sql.insert_many(fields, values_list, update_fields)
        result = execute(sql, auto_commit=True, db=self.__database__)
        if not result:
            self.logger.warning('insert return %s: %s', result, sql)
            return None

        # {
        #     'rows': None,
        #     'conn': <lian.orm.db.PooledConnection object at 0x7fae315a17f0>,
        #     'rowcount': 500,
        #     'description': None,
        #     'lastrowid': 316613
        # }
        excepted_rows = len(values_list)
        if result['rowcount'] != excepted_rows:
            self.logger.warning('insert %s rows (excepted: %s)', result['rowcount'], excepted_rows)
        return result

    def update(self, values, conditions=None):
        sql = self.sql.update(values, conditions=conditions)
        result = execute(sql, auto_commit=True, db=self.__database__)
        return result['rowcount']  # 影响行数

    def count(self, conditions=None):
        sql = self.sql.count(conditions)
        result = query(sql, db=self.__database__)
        return result['rows'][0]['COUNT(1)'] if result else 0

    def delete(self, conditions=None):
        sql = self.sql.delete(conditions)
        result = execute(sql, auto_commit=True, db=self.__database__)
        return result['rowcount']  # 影响行数
