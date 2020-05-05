import json
from typing import Union
from collections import deque

from common.connection import ReplyConnection
from common.request_types import Commands


class Dispatcher:
    def __init__(self, ip: str = '*',
                 port: Union[int, str] = ''):
        self.connection = ReplyConnection(ip, port)
        self.agents = []
        self.broker = None  # NOT IMPLEMENTED
        self._listen = True
        self.request_handler = self.default_request_handler

    def listen(self):
        count = 0
        while self._listen:
            print("waiting")
            self.connection.listen(self.request_handler)

    def default_request_handler(self, request: dict):
        commands = {
            Commands.register: self._register_handler,
            Commands.pulse: self._pulse_handler
        }
        print(f'got command: {request}')
        command = commands[request['command']]
        return command(request)

    def _register_handler(self, request: dict):
        request['id'] = 1
        request['result'] = True
        return request

    def _pulse_handler(self, request: dict):
        request['reply'] = {'status': 'ok'}
        return request


def main():
    dispatcher = Dispatcher()
    dispatcher.listen()


if __name__ == '__main__':
    main()
