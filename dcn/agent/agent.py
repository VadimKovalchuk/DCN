import abc
import logging
import pathlib
import sys
import traceback

from copy import deepcopy
from datetime import datetime
from importlib import import_module

from dcn.common.broker import Broker
from dcn.common.connection import RequestConnection
from dcn.common.constants import AGENT, QUEUE
from dcn.common.data_structures import task_report
from dcn.common.request_types import Agent_queues, Disconnect, Register_agent, Pulse

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
        if reply.get('result', False):
            self.id = reply['id']
            self.sync(reply)
            return True
        else:
            return False

    def request_broker_data(self):
        """
        Request Agent queues on Broker from Dispatcher.
        """
        request = deepcopy(Agent_queues)
        request['token'] = self.token
        request['id'] = self.id
        reply = self.socket.send(request)
        if reply.get('result'):
            self.sync(reply)
            host = reply['broker']['host']
            queue = reply['broker']['queue']
            self.broker = Broker(queue=queue, host=host)
            # self.broker.output_queue = reply['broker']['result']
            return True
        else:
            return False

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
    def __init__(self, task: dict):
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
        if not all(key in self.task for key in ('id', 'client', 'module', 'function', 'arguments')):
            self.update_status(False,
                               f'Mandatory task components are missing')
            return False
        logger.info(
            f"Task ({self.task['id']}) is received from "
            f"{self.task['client']}: {self.task['module']}::{self.task['function']}"
        )
        self.report['id'] = self.task['id']
        self.report['client'] = self.task['client']
        return True

    def get_module(self) -> bool:
        module_name = self.task['module']
        try:
            self._module = import_module(module_name)
            return True
        except ModuleNotFoundError:
            self.update_status(False, f'Module {module_name} is not found')
            return False

    def get_function(self) -> bool:
        function_name = self.task['function']
        try:
            self._function = getattr(self._module, function_name)
            return True
        except AttributeError:
            logger.error(f'Module {self._module} does not contain '
                         f'function {function_name}')
            return False

    def execution(self) -> bool:
        try:
            if self.task['arguments']:
                self.report['result'] = \
                    self._function(self.task['arguments'])
            else:
                self.report['result'] = \
                    self._function()
            self.update_status(True, '')
            return True
        except Exception as e:
            self.update_status(False, traceback.format_exc())
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
