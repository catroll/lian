# -*- coding: utf-8 -*-

import errno
import os

import six


# def mkdir_p(path):
#     try:
#         os.makedirs(path)
#     # Python >2.5 (except OSError, exc: for Python <2.5)
#     except OSError as exc:
#         if exc.errno == errno.EEXIST and os.path.isdir(path):
#             pass
#         else:
#             raise


def mkdir(path, mode=0o777, exist_ok=False):
    if six.PY2:
        try:
            os.makedirs(path, mode)
        # Python >2.5 (except OSError, error: for Python <2.5)
        except OSError as error:
            # if 'File exists' in str(error):
            if error.errno == errno.EEXIST and os.path.isdir(path):
                if exist_ok:
                    return
            raise
    else:
        os.makedirs(path, mode, exist_ok)
