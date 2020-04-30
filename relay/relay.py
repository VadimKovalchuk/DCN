import json
from collections import deque

import zmq

from common.connection import ReplyConnection


agents = []
clients = []
tasks = deque([])


def request_handler(request: str):
    print("Received request: %s" % request)
    return 'World'.encode()


def main():
    connection = ReplyConnection()
    while True:
        print("waiting")
        connection.listen(request_handler, is_json=False)


if __name__ == '__main__':
    main()
