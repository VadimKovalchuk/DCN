import abc
import logging
import pathlib
import sys

from copy import deepcopy
from datetime import datetime
from importlib import import_module

from typing import Callable

from common.broker import Broker, Task
from common.connection import RequestConnection
from common.constants import AGENT, QUEUE
from common.data_structures import task_report
from common.request_types import Agent_queues, Disconnect, Register_agent, Pulse

logger = logging.getLogger(AGENT)

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(f'{parent_path}/modules')


class AgentBase:
    def __init__(self):
        self.id = 0
        self.name = ''
        self.commands = []
        self.token = ''
        self.last_sync = datetime.utcnow()

    @abc.abstractmethod
    def sync(self, reply: dict):
        ...

    def __str__(self):
        return f'Agent: {self.name}({self.id})'


class Agent(AgentBase):
    def __init__(self,
                 token: str,
                 dsp_host: str = 'localhost',
                 dsp_port: int = 9999):
        logger.info('Starting Agent')
        super(Agent, self).__init__()
        self.socket = RequestConnection(dsp_host, dsp_port)
        self.broker = None
        self.token = token

    def __enter__(self):
        self.socket.establish()
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        if self.broker:
            self.broker.close()
        self.socket.close()

    def register(self):
        request = deepcopy(Register_agent)
        if self.name:
            request['name'] = self.name
        request['token'] = self.token
        reply = self.socket.send(request)
        if reply['result']:
            self.id = reply['id']
            self.sync(reply)
        return reply['result']

    def request_broker_data(self):
        """
        Request Agent queues on Broker from Dispatcher.
        """
        request = deepcopy(Agent_queues)
        request['token'] = self.token
        request['id'] = self.id
        reply = self.socket.send(request)
        if reply['result']:
            self.sync(reply)
            self.broker = Broker(reply['broker']['host'])
            self.broker.connect()
            self.broker.declare(reply['broker']['task'],
                                reply['broker']['result'])

    def pulse(self) -> bool:
        request = deepcopy(Pulse)
        request['id'] = self.id
        reply = self.socket.send(request)
        self.sync(reply['reply'])
        return reply['result']

    def sync(self, reply: dict):
        self.last_sync = datetime.utcnow()
        if 'commands' in reply:
            self.commands = reply['commands']

    def apply_commands(self):
        for command in self.commands:
            _method = getattr(self, command)
            if not _method():
                logger.error(f'Method "{_method}" has failed to apply')
                return False
        else:
            return True

    def disconnect(self):
        request = deepcopy(Disconnect)
        request['id'] = self.id
        reply = self.socket.send(request)
        if reply['result']:
            self.broker.close()
            self.broker = None
            self.id = 0
        return reply['result']

    def shutdown(self):
        self.__exit__()
        exit(0)


class RemoteAgent(AgentBase):
    def __init__(self,
                 id_: int = 0):
        super(RemoteAgent, self).__init__()
        self.id = id_

    def sync(self, request: dict):
        self.last_sync = datetime.utcnow()
        if self.commands:
            request['reply']['commands'] = self.commands
        request['result'] = True
        return request


class TaskRunner:
    def __init__(self, task: Task):
        self.task = task
        self.report = deepcopy(task_report)
        self._module = None
        self._function = None
        self.flow = [self.validate_task_parameters, self.get_module, self.get_function, self.execution]

    def update_status(self, status: bool, resolution: str):
        if not status:
            logger.error(resolution)
        self.report['status'] = status
        self.report['resolution'] = resolution

    def validate_task_parameters(self) -> bool:
        """
        Validates task for valid parameters content

        :return:
        bool: validation status (True=passed)
        """
        _task = self.task.body
        if not all(key in _task for key in ('id', 'client', 'module', 'function', 'arguments')):
            self.update_status(False,
                               f'Mandatory task components are missing')
            return False
        logger.info(
            f"Task ({_task['id']}) is received from "
            f"{_task['client'][QUEUE]}: {_task['module']}::{_task['function']}"
        )
        self.report['id'] = self.task.body['id']
        client_queue = self.task.body.get('client')
        if not client_queue.get(QUEUE):
            self.update_status(False,
                               f'Invalid client queue: {client_queue}')
            return False
        self.report['client'] = self.task.body['client']
        # TODO: Validate module is present
        return True

    def get_module(self) -> bool:
        module_name = self.task.body['module']
        try:
            self._module = import_module(module_name)
            return True
        except ModuleNotFoundError:
            self.update_status(False, f'Module {module_name} is not found')
            return False

    def get_function(self) -> bool:
        function_name = self.task.body['function']
        try:
            self._function = getattr(self._module, function_name)
            return True
        except AttributeError:
            logger.error(f'Module {self._module} does not contain '
                         f'function {function_name}')
            return False

    def execution(self) -> bool:
        try:
            if self.task.body['arguments']:
                self.report['result'] = \
                    self._function(self.task.body['arguments'])
            else:
                self.report['result'] = \
                    self._function()
            self.update_status(True, '')
            return True
        except Exception as e:
            self.update_status(False, str(e))
            return False

    def run(self):
        for stage in self.flow:
            logger.debug(f'Starting {stage}')
            if stage():
                logger.debug(f'Complete {stage}')
            else:
                return False
        else:
            logger.info('Command execution is completed')
            return True
