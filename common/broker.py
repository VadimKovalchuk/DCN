import logging
import pika

logger = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)
# RabbitMQ running is required for this module operations
# docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management &


class Broker:
    def __init__(self, host: str):
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
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

    def declare(self, input_queue: str = '', output_queue: str = ''):
        self.input_queue = input_queue
        self.output_queue = output_queue
        logger.debug(f'in {self.input_queue}, out {self.output_queue}')
        if not (self.input_queue or self.output_queue):
            raise RuntimeError('Queue is not defined for Broker')
        for queue in [self.input_queue, self.output_queue]:
            if queue:
                self.channel.queue_declare(queue=queue)

    def push(self, message):
        # TODO: Validate out queue
        logger.debug(f'sending: {message}')
        self.channel.basic_publish(exchange='', routing_key=self.output_queue, body=message)

    def pull(self):
        # TODO: Validate in queue
        for method, properties, body in self.channel.consume(
                self.input_queue, inactivity_timeout=self._inactivity_timeout):
            if not all((method, properties, body)):
                self.channel.cancel()
                break
            logger.debug(" [x] Received %r" % body)
            self.channel.basic_ack(method.delivery_tag)

    def close(self):
        if self.connection:
            self.connection.close()
