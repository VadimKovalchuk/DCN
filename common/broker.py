import json
import logging
from typing import Callable

import pika

logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)
# RabbitMQ running is required for this module operations
# sudo docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.8.4-management &


class Broker:
    def __init__(self, host: str):
        logger.info('Starting broker')
        self.host = host
        self.connection = None
        self.channel = None
        self.input_queue = ''
        self.output_queue = ''
        self._inactivity_timeout = 60  # seconds

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def connect(self):
        logger.info(f'Broker connecting to server: {self.host}')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()

    def declare(self, input_queue: str = '', output_queue: str = ''):
        if not (input_queue or output_queue):
            raise RuntimeError('Queue is not defined for Broker')
        if input_queue:
            self.input_queue = input_queue
            self.channel.queue_declare(queue=input_queue)
        if output_queue:
            self.output_queue = output_queue
            self.channel.queue_declare(queue=output_queue)

    def push(self, message: dict):
        if not self.output_queue:
            raise RuntimeError('Trying to push results to queue '
                               'that not defined')
        msg_str = json.dumps(message, indent=4)
        logger.debug(f'sending: {msg_str}')
        self.channel.basic_publish(exchange='', routing_key=self.output_queue, body=msg_str)

    def pull(self):
        if not self.input_queue:
            raise RuntimeError('Trying to pull task from queue '
                               'that not defined')
        for method, properties, tsk_str in self.channel.consume(
                self.input_queue, inactivity_timeout=self._inactivity_timeout):
            if not all((method, properties, tsk_str)):
                self.channel.cancel()
                break
            logger.debug("Received: %r" % tsk_str)
            try:
                task = json.loads(tsk_str)
            except TypeError:
                logger.error(f'Invalid message content is received: {tsk_str}')
                task = {}
            yield method, properties, task
            self.channel.basic_ack(method.delivery_tag)

    def close(self):
        logger.info('Closing broker connection')
        if self.connection:
            self.connection.close()
