import pika


class Broker:
    def __init__(self, host):
        self.host = host
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.task_queue = ''
        self.result_queue = ''

    def declare(self):
        if self.task_queue:
            self.channel.queue_declare(queue=self.task_queue)
        if self.result_queue:
            self.channel.queue_declare(queue=self.result_queue)

    def push(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.result_queue, body=message)

    def pull(self):
        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)
        self.channel.basic_consume(queue=self.task_queue, auto_ack=True, on_message_callback=callback)

    def consume(self):
        ...
