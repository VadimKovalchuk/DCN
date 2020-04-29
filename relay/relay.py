import json
from collections import deque

import zmq

from common.connection import ReplyConnection


agents = []
clients = []
tasks = deque([])


def main():
    connection = ReplyConnection()
    while True:
        print("waiting")
        message = connection.socket.recv()
        print("Received request: %s" % message)
        #  Do some 'work'
        #  Send reply back to client
        connection.socket.send(b"World")


if __name__ == '__main__':
    main()
