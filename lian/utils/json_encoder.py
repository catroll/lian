# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import datetime
import decimal
import json
import logging
import time

LOG = logging.getLogger(__name__)
INSTALLED_ENCODER = set()


def registry(func):
    INSTALLED_ENCODER.add(func)
    return func


@registry
def datetime_encoder(o):
    if isinstance(o, datetime.datetime):
        yield o.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(o, datetime.date):
        yield o.strftime('%Y-%m-%d')
    if isinstance(o, datetime.time):
        yield o.strftime('%H:%M:%S')


@registry
def magic_encoder(o):
    if hasattr(o, '__json__'):
        yield o.__json__()


class DecimalHack(float):
    def __init__(self, value):
        super(DecimalHack, self).__init__()
        self._value = value

    def __repr__(self):
        return self._value.to_eng_string()


@registry
def decimal_encoder(o):
    if isinstance(o, decimal.Decimal):
        yield DecimalHack(o)


@registry
def set_encoder(o):
    if isinstance(o, set):
        yield list(o)


@registry
def bytes_encoder(o):
    if isinstance(o, bytes):
        # TODO： 统一当作 UTF-8 处理？
        yield o.decode('utf-8')  # 'unicode_escape'


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        for encoder in INSTALLED_ENCODER:
            for result in encoder(o):
                return result
        return super(JsonEncoder, self).default(o)


def json_encode(data, *args, **kwargs):
    start = time.time()
    content = json.dumps(data, cls=JsonEncoder, ensure_ascii=False, *args, **kwargs)
    time_cost = (time.time() - start) * 1000
    LOG.debug('json encode cost %.3f ms' % time_cost)
    return content
