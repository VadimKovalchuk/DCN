from copy import deepcopy
import logging
from typing import Callable, Union

from agent import RemoteAgent
from common.broker import Broker
from common.connection import ReplyConnection
from common.constants import SECOND, QUEUE
from common.data_structures import compose_queue
from common.defaults import INIT_AGENT_ID, RoutingKeys
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
        self._next_free_id = INIT_AGENT_ID
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
        self.broker.setup_exchange()
        self.broker.input_queue = compose_queue(RoutingKeys.DISPATCHER)

    def listen(self, polling_timeout: int = 30 * SECOND):
        while self._listen:
            expired = self.connection.listen(self.request_handler, polling_timeout)
            if self._interrupt and self._interrupt(expired):
                break

    def default_request_handler(self, request: dict):
        commands = {
            Commands.Register: self._register_handler,
            Commands.Pulse: self._pulse_handler,
            Commands.Client_queues: self._client_handler
        }
        assert request['command'] in commands, 'Command ' \
            f'{request["command"]} is not registered in dispatcher request handler'
        command = commands[request['command']]
        return command(request)

    def _register_handler(self, request: dict):
        logger.info(f'Registration request received {request["name"]}')
        request['id'] = self._next_free_id
        agent = RemoteAgent(self._next_free_id)
        agent.name = request['name']
        request['broker']['host'] = self.broker.host
        request['broker']['task'] = compose_queue(RoutingKeys.TASK)
        request['broker']['result'] = compose_queue(RoutingKeys.RESULTS)
        self.agents[self._next_free_id] = agent
        logger.info(f'New agent id={agent.id}')
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
        request['task_queue'] = compose_queue(RoutingKeys.TASK)
        request['result_queue'] = compose_queue(request['name'])
        return request


def main():
    with Dispatcher() as dispatcher:
        dispatcher.listen()


if __name__ == '__main__':
    main()
