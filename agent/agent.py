import abc
import logging

from copy import deepcopy
from datetime import datetime
from importlib import import_module
from typing import Callable

from common.broker import Broker, Task
from common.connection import RequestConnection
from common.constants import AGENT
from common.data_structures import task_report
from common.request_types import Register, Pulse

logger = logging.getLogger(AGENT)


class AgentBase:
    def __init__(self):
        self.id = 0
        self.name = ''
        self.token = ''
        self.last_sync = datetime.utcnow()

    @abc.abstractmethod
    def sync(self, reply: dict):
        ...

    def __str__(self):
        return f'Agent({self.id})'


class Agent(AgentBase):
    def __init__(self,
                 dsp_ip: str = 'localhost',
                 dsp_port: int = 9999):
        logger.info('Starting Agent')
        super(Agent, self).__init__()
        self.socket = RequestConnection(dsp_ip, dsp_port)
        self.broker = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def connect(self):
        self.socket.establish()

    def close(self):
        if self.broker:
            self.broker.close()
        self.socket.close()

    def register(self, callback: Callable = None):
        request = deepcopy(Register)
        if self.name:
            request['name'] = self.name
        reply = self.socket.send(request, callback=callback)
        if reply['result']:
            self.id = reply['id']
            self.sync(reply)
            self.broker = Broker(reply['broker']['host'])
            self.broker.connect()
            self.broker.declare(reply['broker']['task'],
                                reply['broker']['result'])

    def pulse(self, callback: Callable = None) -> bool:
        request = deepcopy(Pulse)
        request['id'] = self.id
        reply = self.socket.send(request, callback=callback)
        self.sync(reply)
        return reply['reply']['status']

    def sync(self, reply: dict):
        self.last_sync = datetime.utcnow()

    def call_task_module(self, command: dict) -> dict:
        module = import_module(f'agent.modules.{command["module"]}')
        func = getattr(module, command['function'])
        result = func(command['arguments'])
        return result

    def __str__(self):
        return f'Agent: {self.name}({self.id})'


class RemoteAgent(AgentBase):
    def __init__(self,
                 id_: int = 0):
        super(RemoteAgent, self).__init__()
        self.id = id_

    def sync(self, request: dict):
        self.last_sync = datetime.utcnow()
        request['reply'] = {'status': 'ok'}
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
        client_queue = self.task.body.get('client')
        if not client_queue or client_queue == 'flush':
            self.update_status(False,
                               f'Invalid client queue name: {client_queue}')
            return False
        module = self.task.body.get('module')
        if not module:
            self.update_status(False, 'Task module is not defined')
            return False
        # TODO: Validate module is present
        function = self.task.body.get('function')
        if not function:
            self.update_status(False, 'Task function is not defined')
            return False
        return True

    def get_module(self) -> bool:
        module_name = self.task.body['module']
        try:
            self._module = import_module(f'agent.modules.{module_name}')
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

    def execution(self) -> bool:
        try:
            self.report['result'] = \
                self._function(self.task.body['arguments'])
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
            return True
