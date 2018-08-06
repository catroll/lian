# -*- coding: utf-8 -*-

import os

import six


def mkdir(path, mode=0o777, exist_ok=False):
    if six.PY2:
        try:
            os.makedirs(path, mode)
        except OSError as error:
            if not exist_ok or 'File exists' not in str(error):
                raise
    else:
        os.makedirs(path, mode, exist_ok)
