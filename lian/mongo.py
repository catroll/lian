# -*- coding: utf-8-*-

"""
MongoDB wrapper

database
collection => table
document   => row
field

client.get_database() : name, codec_options, read_preference, write_concern
                        if no name, return default database
"""

import logging

import motor
from bson.objectid import ObjectId
from tornado import gen

DBS = {}  # 'name': {}

LOG = logging.getLogger(__name__)


class MongoClient(object):
    def __init__(self, logger=None):
        self.logger = logger or LOG

    @staticmethod
    def get_client(name):
        if name not in DBS:
            raise Exception('Get a wrong MongoDB configuration name')

        config = DBS.get(name)
        if 'args' not in config or not isinstance(config['args'], tuple) \
                or 'kwargs' not in config or not isinstance(config['kwargs'], dict):
            raise Exception('Get a wrong MongoDB configuration name')

        if config.get('_conn'):
            config['_conn'] = motor.motor_tornado.MotorClient(*config['args'], **config['kwargs'])
        return config['_conn']

    @gen.coroutine
    def delete(self, name, db, col, _id):
        client = self.get_client(name)
        try:
            yield client[db][col].remove({'_id': ObjectId(_id)})
        except Exception as e:
            self.logger.exception('Delete document from mongo failed [%s,%s,%s,%s]: %s', name, db, col, _id, e)
            raise gen.Return(False)
        raise gen.Return(True)

    def delete_sync(self, name, db, col, _id):
        client = self.get_client(name)
        try:
            client[db][col].remove({'_id': ObjectId(_id)})
        except Exception as e:
            self.logger.exception(e)

    @gen.coroutine
    def read_fail(self):
        res = None
        for name in DBS:
            client = self.get_client(name)
            try:
                res = yield client['sfail']['coll_0'].find_and_modify({}, remove=True)
                if res is not None:
                    res['_id'] = str(res['_id'])
            except Exception as e:
                self.logger.exception(e)
        raise gen.Return(res)
