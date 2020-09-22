import abc
from copy import deepcopy
from datetime import datetime
from time import sleep
from typing import Callable
import logging

from common.broker import Broker
from common.connection import RequestConnection
from common.request_types import Register, Pulse

logger = logging.getLogger(__name__)


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


def main():
    connection = RequestConnection(port=9999)
    print("Connecting to dispatcher")
    reply = connection.send(Register)
    print(reply)
    if reply and reply['result']:
        agent_id = reply['id']
    else:
        assert False, 'Failed to get ID'
    _pulse = deepcopy(Pulse)
    _pulse['id'] = agent_id
    #  Do 10 requests, waiting each time for a response
    for request in range(10):
        print("Sending pulse: %s" % request)
        message = connection.send(_pulse)
        print("Received pulse reply %s [ %s ]" % (request, message))
        sleep(1)


if __name__ == '__main__':
    main()
