import json
import logging
from typing import Generator, Union

import pika
from pika.exceptions import AMQPConnectionError

from common.constants import BROKER, EXCHANGE, QUEUE, SECOND
from common.defaults import EXCHANGE_NAME, EXCHANGE_TYPE, RoutingKeys

logger = logging.getLogger(BROKER)
logging.getLogger('pika').setLevel(logging.WARNING)
# RabbitMQ running container is required for current module operations
# sudo docker run -dit --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9.11-management


def validate_task(method, properties, task_str):
    """
    Validate all task parameters are defined and
    task string can be parsed as json.
    """
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
    """
    RabbitMQ broker connection wrapper.
    """
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

    @property
    def connected(self):
        if self.connection and \
                self.channel and \
                self.input_queue:
            return True
        else:
            return False

    def connect(self):
        """
        Establish connection to broker server.
        """
        logger.info(f'Broker connecting to server: {self.host}')
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host))
        except AMQPConnectionError:
            logger.error(f'Broker server is not reachable.')
            return False
        else:
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)
            return True

    def setup_exchange(self,
                       ex_name: str = EXCHANGE_NAME,
                       ex_type: str = EXCHANGE_TYPE):
        """
        Configures exchange point on broker and default queues on it.

        :param ex_name: Exchange name
        :param ex_type: Exchange type
        """
        self.channel.exchange_declare(exchange=ex_name,
                                      exchange_type=ex_type)
        for queue in RoutingKeys.ALL_QUEUES:
            self.channel.queue_declare(queue)
            self.channel.queue_bind(exchange=EXCHANGE_NAME,
                                    queue=queue,
                                    routing_key=queue)

    def declare(self,
                input_queue: Union[dict, None] = None,
                output_queue: Union[dict, None] = None):
        """
        Declares input and output queues defined for broker instance

        :param input_queue: queue that should be set for broker as input
        :param output_queue: queue that should be set for broker as output
        """
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

    def push(self, message: dict, queue: Union[None, dict] = None):
        """
        Push message to output queue or custom queue that can be passed
        as optional input parameter.

        :param message: message body
        :param queue: (optional) target queue
        """
        if not self.output_queue and not queue:
            raise RuntimeError('Trying to push results to queue '
                               'that not defined')
        _queue = queue or self.output_queue
        msg_str = json.dumps(message, indent=4)
        logger.debug(f'sending: {msg_str}')
        self.channel.basic_publish(exchange=_queue[EXCHANGE],
                                   routing_key=_queue[QUEUE],
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
