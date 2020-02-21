import zmq

context = zmq.Context()

#  Socket to talk to server
# print("Connecting to hello world server…")
# socket = context.socket(zmq.REQ)
# socket.connect("tcp://localhost:9999")


class RelayHandler:
    def __init__(self):
        self.ip = 'localhost'
        self.port = '9999'

    def register(self):
        print(f'Register on Relay {self.ip}:{self.port}')
        socket = context.socket(zmq.REQ)
        socket.connect(f'tcp://{self.ip}:{self.port}')

#  Do 10 requests, waiting each time for a response
for request in range(10):
    print("Sending request %s …" % request)
    socket.send(b"Hello")

    #  Get the reply.
    message = socket.recv()
    print("Received reply %s [ %s ]" % (request, message))