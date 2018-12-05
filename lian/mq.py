# -*- coding: utf-8 -*-

import json
import logging
import threading
import time

from six.moves import queue

import pika

AUTO_RECOVER = 60
UPDATE_INTERVAL = 10

LOG = logging.getLogger(__name__)


class MQInstance(object):
    _connection = None
    _channel = None

    def __init__(self, connection_parameters, mq_queue):
        """ init

        Args:
            connection_parameters: host port
            mq_queue: str, MQ queue name
            logger:
            logger_mq:
        """
        self.mq_conn_params = connection_parameters
        self.mq_queue = mq_queue
        self.beat = json.dumps({'beat': 1})
        self.message_queue = queue.Queue()
        self._thread = None

    def connect(self):
        LOG.info("connecting MQ server...")
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(**self.mq_conn_params))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.mq_queue, durable=True)
        LOG.info("MQ connected")

    def reconnect(self):
        try:
            self.connect()
        except Exception as e:
            LOG.exception(e)

    def produce(self, msg_str):
        self.message_queue.put(msg_str)
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._produce)
            self._thread.start()

    def _produce(self):
        while True:
            try:
                msg_str = self.message_queue.get(False)
            except queue.Empty:
                msg_str = self.beat
                time.sleep(5)

            while True:
                try:
                    self._channel.basic_publish(exchange='',
                                                routing_key=self.mq_queue,
                                                body=msg_str,
                                                properties=pika.BasicProperties(delivery_mode=2, ))
                    break
                except Exception as e:
                    LOG.exception(e)
                    self.close()
                    time.sleep(1)
                    self.reconnect()

    def close(self):
        try:
            if self._channel:
                self._channel.close()
            if self._connection:
                self._connection.close()
        except Exception as e:
            LOG.exception(e)
