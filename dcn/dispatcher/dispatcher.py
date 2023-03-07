import logging
from time import monotonic
from typing import Callable, Union

from dcn.agent.agent import RemoteAgent
from dcn.common.broker import Broker
from dcn.common.connection import ReplyConnection
from dcn.common.constants import DISPATCHER, SECOND
# from dcn.common.data_structures import compose_queue
from dcn.common.defaults import EXCHANGE_NAME, INIT_AGENT_ID, RoutingKeys
from dcn.common.request_types import Commands
from dcn.common.database import Database

logger = logging.getLogger(DISPATCHER)


class Dispatcher:
    def __init__(self,
                 ip: str = '*',
                 port: Union[int, str] = '',
                 broker_host: str = ''):
        logger.info('Starting Dispatcher')
        self.socket = ReplyConnection(ip, port)
        self.broker = Broker(broker_host if broker_host else ip)
        self.agents = {}
        self.request_handler = self.default_request_handler
        self._next_free_id = INIT_AGENT_ID
        self._listen = True
        self._interrupt: Union[None, Callable] = None

    def __enter__(self):
        self.socket.establish()
        self.configure_broker()
        return self

    def __exit__(self, *exc_info):
        logger.info(f'Closing Dispatcher connection:{self.socket}')
        self.socket.close()
        self.broker.close()

    def configure_broker(self):
        self.broker.queue = RoutingKeys.DISPATCHER
        self.broker.routing_key = RoutingKeys.DISPATCHER
        for i in range(12):
            if self.broker.connect():
                return

    def listen(self, polling_timeout: int = 60 * SECOND):
        ts = monotonic()
        while self._listen:
            expired = self.socket.listen(self.request_handler, polling_timeout)
            if self._interrupt and self._interrupt(expired):
                break
            if monotonic() > ts + 60 * SECOND:
                if not self.broker.is_connected:
                    self.configure_broker()
                else:
                    for task in self.broker.consume():
                        logger.warning(f'Got dispatcher task {task}')
                ts = monotonic()

    def default_request_handler(self, request: dict):
        commands = {
            Commands.Register_agent: self._register_agent_handler,
            Commands.Agent_queues: self._agent_queues_handler,
            Commands.Pulse: self._pulse_handler,
            Commands.Client_queues: self._client_handler,
            Commands.Relay: self._relay,
            Commands.Disconnect: self._disconnect_handler
        }
        assert request['command'] in commands, 'Command ' \
            f'{request["command"]} is not registered in dispatcher request handler'
        command = commands[request['command']]
        return command(request)

    def _register_agent_handler(self, request: dict):
        logger.info(f'Registration request received {request["name"]}({self._next_free_id})')
        request['id'] = self._next_free_id
        agent = RemoteAgent(self._next_free_id)
        agent.name = request['name']
        agent.token = request['token']
        self.agents[self._next_free_id] = agent
        logger.info(f'New agent id={agent.id}')
        request['result'] = True
        self._next_free_id += 1
        return request

    def _agent_queues_handler(self, request: dict):
        """
        Returns Host and queues that agent should connect to.
        """
        agent = self.agents[request['id']]
        logger.info(f'Agent queues request received from {agent}')
        if self.broker.is_connected:
            config = Database.get_agent_param(agent.token)
            request['broker']['host'] = config['broker']
            request['broker']['queue'] = RoutingKeys.TASK
            # request['broker']['exchange'] = EXCHANGE_NAME
            # request['broker']['result'] = compose_queue(RoutingKeys.RESULTS)
            request['result'] = True
        return request

    def _pulse_handler(self, request: dict):
        logger.info(f'Pulse request received {request["id"]}')
        agent = self.agents[request['id']]
        reply = agent.sync(request)
        return reply

    def _client_handler(self, request: dict):
        logger.info(f'Client queues are requested by: {request["name"]}')
        if self.broker.is_connected:
            config = Database.get_client_param(request['token'])
            request['broker']['host'] = config['broker']
            request['broker']['task'] = compose_queue(RoutingKeys.TASK)
            request['broker']['result'] = compose_queue(request['name'])
            request['result'] = True
        return request

    def _disconnect_handler(self, request: dict):
        """
        Removes agent instance on dispatcher.
        """
        logger.info(f'Disconnect request received {request["id"]}')
        if request['id'] in self.agents:
            self.agents.pop(request['id'])
        request['result'] = True
        return request

    def _relay(self, request: dict):
        """
        Returns received request.
        """
        logger.debug(f'Relay request called')
        return request
