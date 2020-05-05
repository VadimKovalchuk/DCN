import json
from typing import Union
from collections import deque

from common.connection import ReplyConnection
from common.request_types import Commands


def register(request: dict):
    request['id'] = 1
    request['result'] = True
    return request


def pulse(request: dict):
    request['reply'] = {'status': 'ok'}
    return request


commands = {
    Commands.register: register,
    Commands.pulse: pulse
}


def request_handler(request: dict):
    print(f'got command: {request}')
    command = commands[request['command']]
    return command(request)


class Dispatcher:
    def __init__(self, ip: str = '*',
                 port: Union[int, str] = ''):
        self.connection = ReplyConnection(ip, port)
        self.agents = []
        self.broker = None  # NOT IMPLEMENTED
        self._listen = True
        self.request_handler = request_handler
        self._listen_count = 0

    def listen(self):
        count = 0
        while self._listen:
            count += 1
            print("waiting")
            self.connection.listen(self.request_handler)
            if count == self._listen_count:
                self._listen = False


def main():
    dispatcher = Dispatcher()
    dispatcher.listen()


if __name__ == '__main__':
    main()
