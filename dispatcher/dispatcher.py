import logging
from typing import Callable, Union

from agent import RemoteAgent
from common.connection import ReplyConnection
from common.broker import Broker
from common.request_types import Commands

logger = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self,
                 ip: str = '*',
                 port: Union[int, str] = '',
                 broker_host: str = ''):
        logger.info('Starting Dispatcher')
        self.connection = ReplyConnection(ip, port)
        self.broker = Broker(broker_host if broker_host else ip)
        self.agents = {}
        self.request_handler = self.default_request_handler
        self._next_free_id = 1001
        self._listen = True
        self._interrupt: Union[None, Callable] = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        logger.info(f'Closing Dispatcher connection:{self.connection}')
        self.connection.close()
        self.broker.close()

    def connect(self):
        self.connection.establish()
        self.broker.connect()
        self.broker.declare('client_requested', 'task')

    def listen(self, polling_timeout: int = 30):
        while self._listen:
            expired = self.connection.listen(self.request_handler, polling_timeout)
            if self._interrupt and self._interrupt(expired):
                break

    def default_request_handler(self, request: dict):
        commands = {
            Commands.register: self._register_handler,
            Commands.pulse: self._pulse_handler,
            Commands.client_queues: self._client_handler
        }
        assert request['command'] in commands, 'Command ' \
            f'{request["command"]} is not registered in dispatcher request handler'
        command = commands[request['command']]
        return command(request)

    def _register_handler(self, request: dict):
        logger.debug(f'Registration request received {request["name"]}')
        request['id'] = self._next_free_id
        agent = RemoteAgent(self._next_free_id)
        agent.name = request['name']
        request['broker']['host'] = self.broker.host
        request['broker']['task'] = 'task'
        request['broker']['result'] = 'result'
        self.agents[self._next_free_id] = agent
        request['result'] = True
        self._next_free_id += 1
        return request

    def _pulse_handler(self, request: dict):
        logger.debug(f'Pulse request received {request["id"]}')
        agent = self.agents[request['id']]
        reply = agent.sync(request)
        return reply

    def _client_handler(self, request: dict):
        logger.debug(f'Client queues are requested by: {request["name"]}')
        request['broker'] = self.broker.host
        request['task_queue'] = 'client_requested'
        request['result_queue'] = request['name']
        return request


def main():
    with Dispatcher() as dispatcher:
        dispatcher.listen()


if __name__ == '__main__':
    main()
