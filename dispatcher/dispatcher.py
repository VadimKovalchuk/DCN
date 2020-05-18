import logging
from typing import Callable, Union

from agent import RemoteAgent
from common.connection import ReplyConnection
from common.request_types import Commands

logger = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self, ip: str = '*',
                 port: Union[int, str] = ''):
        logger.info('Starting Dispatcher')
        self.connection = ReplyConnection(ip, port)
        self.broker = None  # NOT IMPLEMENTED
        self.agents = {}
        self.request_handler = self.default_request_handler
        self._next_free_id = 1001
        self._listen = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        logger.info(f'Closing Dispatcher connection:{self.connection}')
        self.connection.close()

    def listen(self, polling_timeout: int = 30, interrupt: Callable = None):
        while self._listen:
            expired = self.connection.listen(self.request_handler, polling_timeout)
            if interrupt and interrupt(expired):
                break

    def default_request_handler(self, request: dict):
        commands = {
            Commands.register: self._register_handler,
            Commands.pulse: self._pulse_handler
        }
        command = commands[request['command']]
        return command(request)

    def _register_handler(self, request: dict):
        logger.debug(f'Registration request received {request["name"]}')
        request['id'] = self._next_free_id
        self.agents[self._next_free_id] = RemoteAgent(self._next_free_id)
        request['result'] = True
        self._next_free_id += 1
        return request

    def _pulse_handler(self, request: dict):
        logger.debug(f'Pulse request received {request["id"]}')
        agent = self.agents[request['id']]
        reply = agent.sync(request)
        return reply


def main():
    with Dispatcher() as dispatcher:
        dispatcher.listen()


if __name__ == '__main__':
    main()
