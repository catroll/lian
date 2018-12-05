# -*- coding: utf-8 -*-

"""
https://gist.github.com/simon-weber/7755289
"""

import functools
import threading

import tornado.stack_context


def global_context(**kwargs):
    return tornado.stack_context.StackContext(functools.partial(ThreadContext, **kwargs))


class ThreadContextMetaclass(type):
    _state = threading.local()
    _state.data = {}

    @property
    def data(cls):
        if not hasattr(cls._state, 'data'):
            return {}
        return cls._state.data

    def get(cls, item, default=None):
        return cls.data.get(item, default)

    def __getitem__(cls, item):
        return cls.data[item]


class ThreadContext(object):
    __metaclass__ = ThreadContextMetaclass

    def __init__(self, **data):
        self._data = data

    def __enter__(self):
        self._prev_data = self.__class__.data
        self.__class__._state.data = self._data

    def __exit__(self, *exc):
        self.__class__._state.data = self._prev_data
        del self._prev_data
        return False


__all__ = 'ThreadContext', 'global_context',


def __test():
    """
    for i in {1..100}; do curl -s http://localhost:8888/; echo; done;
    ab -c 100 -n 10000 http://localhost:8888/
    """

    import tornado.web
    import tornado.ioloop
    import tornado.stack_context

    class RequestContextHandler(tornado.web.RequestHandler):
        def _execute(self, transforms, *args, **kwargs):
            global_data = {'request': self.request}
            with global_context(**global_data):
                super(RequestContextHandler, self)._execute(transforms, *args, **kwargs)

    class MainHandler(RequestContextHandler):
        def get(self):
            self.write('Hello, %s' % ThreadContext['request'].remote_ip)

    def make_app():
        return tornado.web.Application([
            (r'/', MainHandler),
        ])

    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    __test()
