import json
import logging
from time import sleep
from typing import Callable, Generator

import pika
from pika.exceptions import AMQPConnectionError

CONNECTION_RETRY_COUNT = 5
RECONNECT_DELAY = 5  # seconds

logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)
# RabbitMQ running is required for this module operations
# sudo docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.8.4-management &


def validate_task(method, properties, task_str):
    if all((method, properties, task_str)):
        try:
            json.loads(task_str)
        except TypeError:
            logger.error(f'Invalid message content is received: {task_str}')
            return False
        else:
            return True


class Task:
    def __init__(self, method: pika.spec.Basic.Deliver,
                 properties: pika.spec.BasicProperties,
                 body: str):
        self.method = method
        self.properties = properties
        self.body = json.loads(body)


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
        for _try in range(CONNECTION_RETRY_COUNT):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host))
            except AMQPConnectionError:
                logger.error(f'Attempt #{_try + 1} has failed.')
                sleep(RECONNECT_DELAY)
            else:
                break
        else:
            message = 'Broker server is not reachable.'
            logger.error(message)
            raise ConnectionError(message)
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

    def pulling_generator(self) -> Generator:
        if not self.input_queue:
            raise RuntimeError('Trying to pull task from queue '
                               'that not defined')
        for method, properties, task_str in self.channel.consume(
                self.input_queue, inactivity_timeout=self._inactivity_timeout):
            logger.debug("Received: %r" % task_str)
            if validate_task(method, properties, task_str):
                yield Task(method, properties, task_str)
            else:
                self.channel.cancel()
                break

    def set_task_done(self, task: Task):
        self.channel.basic_ack(task.method.delivery_tag)

    def close(self):
        logger.info('Closing broker connection')
        if self.connection:
            self.connection.close()


