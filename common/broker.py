import json
import logging
from time import sleep
from typing import Generator, Union

import pika
from pika.exceptions import AMQPConnectionError

from common.constants import BROKER, EXCHANGE, QUEUE, SECOND
from common.defaults import CONNECTION_RETRY_COUNT, RECONNECT_DELAY,\
    EXCHANGE_NAME, EXCHANGE_TYPE, RoutingKeys

logger = logging.getLogger(BROKER)
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

    def __str__(self):
        return json.dumps(self.body, indent=4)


class Broker:
    def __init__(self, host: str):
        logger.info('Starting broker')
        self.host = host
        self.connection = None
        self.channel = None
        self.input_queue = ''
        self.output_queue = ''
        self._inactivity_timeout = 10 * SECOND

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
        self.channel.basic_qos(prefetch_count=1)

    def setup_exchange(self,
                       ex_name: str = EXCHANGE_NAME,
                       ex_type: str = EXCHANGE_TYPE):
        self.channel.exchange_declare(exchange=ex_name,
                                      exchange_type=ex_type)
        for queue in RoutingKeys.ALL_QUEUES:
            self.channel.queue_declare(queue)
            self.channel.queue_bind(exchange=EXCHANGE_NAME,
                                    queue=queue,
                                    routing_key=queue)

    def declare(self, input_queue: Union[dict, None] = None,
                output_queue: Union[dict, None] = None):
        if not (input_queue or output_queue):
            raise RuntimeError('Queue is not defined for Broker')
        if input_queue:
            in_q = input_queue[QUEUE]
            self.input_queue = input_queue
            self.channel.queue_declare(queue=in_q)
            self.channel.queue_bind(exchange=input_queue[EXCHANGE],
                                    queue=in_q,
                                    routing_key=in_q)
        if output_queue:
            self.output_queue = output_queue

    def push(self, message: dict):
        if not self.output_queue:
            raise RuntimeError('Trying to push results to queue '
                               'that not defined')
        msg_str = json.dumps(message, indent=4)
        logger.debug(f'sending: {msg_str}')
        self.channel.basic_publish(exchange=self.output_queue[EXCHANGE],
                                   routing_key=self.output_queue[QUEUE],
                                   body=msg_str)

    def pulling_generator(self) -> Generator:
        if not self.input_queue:
            raise RuntimeError('Trying to pull task from queue '
                               'that not defined')
        for method, properties, task_str in self.channel.consume(
                self.input_queue[QUEUE],
                inactivity_timeout=self._inactivity_timeout):
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


