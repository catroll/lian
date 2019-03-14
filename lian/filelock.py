"""
Original: https://raw.githubusercontent.com/benediktschmitt/py-filelock/eda476814312dd815f566991676a787be22b1e67/filelock.py
License: Public Domain
"""

import fcntl
import logging
import os
import threading
import time

__all__ = [
    "Timeout",
    "FileLock"
]


_logger = None


def logger():
    global _logger
    _logger = _logger or logging.getLogger(__name__)
    return _logger


class Timeout(OSError):  # PY3: TimeoutError
    def __init__(self, lock_file):
        self.lock_file = lock_file
        return None

    def __str__(self):
        temp = "The file lock '{}' could not be acquired."\
               .format(self.lock_file)
        return temp


class _Acquire_ReturnProxy(object):

    def __init__(self, lock):
        self.lock = lock
        return None

    def __enter__(self):
        return self.lock

    def __exit__(self, exc_type, exc_value, traceback):
        self.lock.release()
        return None


class FileLock(object):
    def __init__(self, lock_file, timeout=-1):
        self._lock_file = lock_file
        self._lock_file_fd = None
        self.timeout = timeout
        self._thread_lock = threading.Lock()
        self._lock_counter = 0
        return None

    @property
    def lock_file(self):
        return self._lock_file

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = float(value)
        return None

    @property
    def is_locked(self):
        return self._lock_file_fd is not None

    def acquire(self, timeout=None, poll_intervall=0.05):
        if timeout is None:
            timeout = self.timeout

        with self._thread_lock:
            self._lock_counter += 1

        lock_id = id(self)
        lock_filename = self._lock_file
        start_time = time.time()
        try:
            while True:
                with self._thread_lock:
                    if not self.is_locked:
                        logger().debug('Attempting to acquire lock %s on %s', lock_id, lock_filename)
                        self._acquire()

                if self.is_locked:
                    logger().info('Lock %s acquired on %s', lock_id, lock_filename)
                    break
                elif timeout >= 0 and time.time() - start_time > timeout:
                    logger().debug('Timeout on acquiring lock %s on %s', lock_id, lock_filename)
                    raise Timeout(self._lock_file)
                else:
                    logger().debug(
                        'Lock %s not acquired on %s, waiting %s seconds ...',
                        lock_id, lock_filename, poll_intervall
                    )
                    time.sleep(poll_intervall)
        except:
            with self._thread_lock:
                self._lock_counter = max(0, self._lock_counter - 1)
            raise
        return _Acquire_ReturnProxy(lock=self)

    def release(self, force=False):
        with self._thread_lock:
            if self.is_locked:
                self._lock_counter -= 1
                if self._lock_counter == 0 or force:
                    lock_id = id(self)
                    lock_filename = self._lock_file
                    logger().debug('Attempting to release lock %s on %s', lock_id, lock_filename)
                    self._release()
                    self._lock_counter = 0
                    logger().info('Lock %s released on %s', lock_id, lock_filename)
        return None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        return None

    def __del__(self):
        self.release(force=True)
        return None

    def _acquire(self):
        open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        fd = os.open(self._lock_file, open_mode)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            os.close(fd)
        else:
            self._lock_file_fd = fd
        return None

    def _release(self):
        fd = self._lock_file_fd
        self._lock_file_fd = None
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        return None
