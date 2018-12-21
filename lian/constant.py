# -*- coding: utf-8 -*-

import re

EMPTY_MAPPING = {}
RE_CONSTANT_NAME = re.compile('^[A-Z_]+$')


class Constant:
    # __names__ = filter(RE_CONSTANT_NAME.match, locals().keys())
    __mapping__ = EMPTY_MAPPING

    @classmethod
    def initialize(cls):
        if cls.__mapping__ is EMPTY_MAPPING:
            cls.__mapping__ = {k: v for k, v in cls.__dict__.items() if RE_CONSTANT_NAME.match(k)}

    @classmethod
    def __contains__(cls, item):
        cls.initialize()
        return item in cls.__mapping__

    @classmethod
    def is_valid(cls, item):
        cls.initialize()
        return item in cls.__mapping__.values()
