import logging
import pika

logger = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)


class Broker:
    def __init__(self, host):
        self.host = host
        self.connection = None
        self.channel = None
        self.input_queue = ''
        self.output_queue = ''

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

    def declare(self):
        if self.input_queue:
            self.channel.queue_declare(queue=self.input_queue)
        if self.output_queue:
            self.channel.queue_declare(queue=self.output_queue)

    def push(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.output_queue, body=message)

    def pull(self):
        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)
        self.channel.basic_consume(queue=self.input_queue, auto_ack=True, on_message_callback=callback)

    def consume(self):
        ...

    def close(self):
        if self.connection:
            self.connection.close()
