# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import logging
import threading
import time

import redis

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = '0'
REDIS_CACHE_PERIOD = 60 * 5

__all__ = 'Redis',
LOG = logging.getLogger(__name__)


class StrictRedisWrapper(redis.StrictRedis):
    def execute_command(self, *args, **options):
        start_time = time.time()
        try:
            return super(StrictRedisWrapper, self).execute_command(*args, **options)
        finally:
            time_cost = (time.time() - start_time) * 1000
            if time_cost > 50:
                LOG.warning('redis slow: %s, cost: %f', args, time_cost)


class Redis(object):
    _instance_lock = threading.Lock()

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                 decode_responses=True, cache_period=REDIS_CACHE_PERIOD):
        self.redis = StrictRedisWrapper(host=host, port=port, db=db, decode_responses=decode_responses)
        self.pipe = self.redis.pipeline()
        self.cache_period = cache_period

    @staticmethod
    def instance():
        if not hasattr(Redis, '_instance'):
            with Redis._instance_lock:
                if not hasattr(Redis, '_instance'):
                    Redis._instance = Redis()
        return Redis._instance

    def get(self, key):
        return self.redis.get(key)

    def lrange(self, name, start, end):
        # return self.redis.(name,key)
        return self.redis.lrange(name, start, end)

    def delete(self, key):
        return self.redis.delete(key)

    def set(self, key, value, seconds=None):
        if seconds is None:
            seconds = self.cache_period
        self.redis.set(key, value, seconds)

    def set_no_expire(self, key, value):
        self.redis.set(key, value)

    def set_list(self, key, value, seconds=None):
        pipe = self.redis.pipeline()
        pipe.delete(key)
        for item in value:
            pipe.rpush(key, item)
        if seconds:
            pipe.expire(key, seconds)
        pipe.execute()
        pipe.reset()

    def get_list(self, key):
        res = self.redis.lrange(key, 0, -1)
        return [i.decode('utf-8') for i in res] if res else []

    def push(self, key, value):
        pipe = self.redis.pipeline()
        pipe.rpush(key, value)
        pipe.execute()
        pipe.reset()

    def hpop(self, name):
        keys = self.redis.hkeys(name)
        if len(keys) > 0:
            val = self.redis.hget(name, keys[0])
            self.redis.hdel(name, keys[0])
            if val:
                return val.decode('utf-8')
        return None

    def hset(self, name, key, value):
        self.redis.hset(name, key, value)

    def keys(self, name):
        return self.redis.scan(cursor=0, match=name, count=1000)[1]

    def mget(self, keys, *args):
        return self.redis.mget(keys, *args)

    def hmget(self, name, keys, *args):
        return self.redis.hmget(name, keys, *args)

    def gets(self, keys, as_list=False):
        if isinstance(keys, str):
            keys = self.keys(keys)
        for key in keys:
            if as_list:
                self.pipe.lrange(key, 0, -1)
            else:
                self.pipe.get(key)
        return self.pipe.execute()
