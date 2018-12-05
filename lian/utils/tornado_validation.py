# -*- encoding: utf-8 -*-

# original: https://gist.githubusercontent.com/anonymous/850704
# modified by: catroll <ninedoors@126.com>

import datetime
import re

TRUE_VALUES = ['1', 'true', 'yes', 'on']
FALSE_VALUES = ['0', 'false', 'no', 'off']

NoDefaultValue = object()
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_TIME_FORMAT = '%H:%M:%S'
DEFAULT_DATETIME_FORMAT = '%s %s' % (DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT)


class ValidationMixin:
    errors = None

    MSG_DEFAULT = u'is not valid'
    MSG_REQUIRED = u'is required'
    MSG_MUST_MATCH = u" doesn't match"

    ALPHA = r'^\D+$'
    NUMERIC = r'^\d+$'
    EMAIL = r'^[a-zA-Z0-9._%-+]+\@[a-zA-Z0-9._%-]+\.[a-zA-Z]{2,}$'
    USERNAME = r'^[a-z0-9._]{3,}$'

    def _help(self, specified, name, msg):
        if specified is not None:
            self.errors[name] = specified
        else:
            self.errors[name] = name + ' ' + msg

    @staticmethod
    def _get_arg_name(arg_name, name=None):
        if name:
            return name
        return arg_name.replace('_', ' ').capitalize()

    def _get_arg_error(self, arg_name, name=None):
        return self.errors.get(self._get_arg_name(arg_name, name), None)

    def valid(self, arg_name, validator=None, help=None, name=None, required=True, default=NoDefaultValue, **kwargs):
        value = self.get_argument(arg_name)
        if value == self._ARG_DEFAULT and default != NoDefaultValue:
            return default

        assert isinstance(value, (str, unicode)), 'HTTP Request Parameter should be a string type'

        if not hasattr(self, 'errors'):
            self.errors = {}
        if not required and not validator:
            return value
        if arg_name in self.errors:
            return value

        name = self._get_arg_name(arg_name, name)
        if required and (value is None or len(value) == 0):
            self._help(help, name, ValidationMixin.MSG_REQUIRED)
        elif isinstance(validator, type(self.valid)):  # 方法
            extra_params = kwargs.get('extra_params', {})
            value = validator(value, **extra_params)
            if not value:
                self._help(help, name, ValidationMixin.MSG_DEFAULT)
        elif isinstance(validator, type):
            # int, float, bool, str, decimal.Decimal, datetime.datetime, datetime.date
            if validator == bool:
                _value = value.lower()
                if _value in TRUE_VALUES:
                    value = True
                elif _value in FALSE_VALUES:
                    value = False
                else:
                    value = default if isinstance(default, bool) else None
            elif validator == datetime.date:
                try:
                    _format = kwargs.get('format', DEFAULT_DATE_FORMAT)
                    value = datetime.datetime.strptime(value, _format)
                except ValueError:
                    self._help(help, name, ValidationMixin.MSG_DEFAULT)
            elif validator == datetime.datetime:
                try:
                    _format = kwargs.get('format', DEFAULT_DATETIME_FORMAT)
                    value = datetime.datetime.strptime(value, _format)
                except ValueError:
                    self._help(help, name, ValidationMixin.MSG_DEFAULT)
            else:
                try:
                    value = validator(value)
                except ValueError:
                    self._help(help, name, ValidationMixin.MSG_DEFAULT)
        elif isinstance(validator, str):
            value2 = self.get_argument(validator, None)
            if value2 is not None:
                if value != value2:
                    self._help(help, name, ValidationMixin.MSG_MUST_MATCH)
            else:
                # 正则表达式
                if re.match(validator, value, re.IGNORECASE) is None:
                    self._help(help, name, ValidationMixin.MSG_DEFAULT)
        return value
