import functools


class LoggerWrap(object):
    def __init__(self, logger):
        self.logger = logger

    def log(self, level, *args, **kwargs):
        if self.logger:
            getattr(self.logger, level)(*args, **kwargs)

    def __getattr__(self, item):
        if item in ('debug', 'info', 'warn', 'warning', 'error', 'fatal', 'critical', 'exception'):
            return functools.partial(self.log, level=item)
