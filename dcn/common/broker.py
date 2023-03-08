import json
import logging
from time import sleep
from typing import Generator, Union

import pika
from pika.exceptions import AMQPConnectionError

from dcn.common.constants import BROKER, EXCHANGE, QUEUE, SECOND
from dcn.common.defaults import EXCHANGE_NAME, EXCHANGE_TYPE, RoutingKeys

logger = logging.getLogger(BROKER)
logging.getLogger('pika').setLevel(logging.WARNING)
# RabbitMQ running container is required for current module operations
# sudo docker run -dit --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.11-management


class Broker:
    def __init__(self,
                 exchange=EXCHANGE_NAME,
                 exchange_type=EXCHANGE_TYPE,
                 routing_key='',
                 queue='',
                 host='localhost',
                 # credentials=None
                 ):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key if routing_key else queue
        self.output_routing_key = None
        self.queue = queue
        self.host = host
        # self.credentials = credentials
        self.is_connected = False
        self._connection = None
        self._channel = None

    def connect(self):
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    # credentials=self.credentials
                )
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
            self._channel.queue_declare(queue=self.queue)
            self._channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)
            self._channel.basic_qos(prefetch_count=1)
            self.is_connected = True
            return True
        except pika.exceptions.AMQPConnectionError:
            print('Unable to connect to RabbitMQ broker')
            return False

    def publish(self, message: dict, routing_key: str = None):
        try:
            msg_str = json.dumps(message, indent=4)
            logger.debug(f'sending: {msg_str}')
            self._channel.basic_publish(
                exchange=self.exchange,
                routing_key=routing_key if routing_key else self.output_routing_key,
                body=msg_str
            )
            return True
        except pika.exceptions.AMQPConnectionError:
            self.is_connected = False
            print('Lost connection to RabbitMQ while publishing message')
            return False

    def consume(self):
        try:
            method_frame, header_frame, body = self._channel.basic_get(queue=self.queue, auto_ack=True)
            logger.debug(f'Message received: {body}')
            if body:
                return True, json.loads(body)
            else:
                return True, {}
        except pika.exceptions.AMQPConnectionError:
            self.is_connected = False
            print('Lost connection to RabbitMQ while consuming message')
            return False, {}

    def close(self):
        if self._connection is not None and not self._connection.is_closed:
            self._connection.close()
        self.is_connected = False
