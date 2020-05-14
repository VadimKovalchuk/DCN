import abc
from copy import deepcopy
from datetime import datetime
from time import sleep

from common.connection import RequestConnection
from common.request_types import register, pulse


class AgentBase:
    def __init__(self):
        self.id = 0
        self.last_sync = datetime.utcnow()

    @abc.abstractmethod
    def sync(self):
        ...

    def __str__(self):
        return f'Agent({self.id})'


class Agent(AgentBase):
    def __init__(self,
                 dsp_ip: str = 'localhost',
                 dsp_port: int = 9999):
        super(Agent, self).__init__()
        self.socket = RequestConnection(dsp_ip, dsp_port)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.socket.close()

    def sync(self, reply: dict):
        self.last_sync = datetime.utcnow()


class RemoteAgent(AgentBase):
    def __init__(self,
                 id_: int = 0):
        super(RemoteAgent, self).__init__()
        self.id = id_

    def sync(self, request: dict):
        self.last_sync = datetime.utcnow()
        return request


def main():
    connection = RequestConnection(port=9999)
    print("Connecting to dispatcher")
    reply = connection.send(register)
    print(reply)
    if reply and reply['result']:
        agent_id = reply['id']
    else:
        assert False, 'Failed to get ID'
    _pulse = deepcopy(pulse)
    _pulse['id'] = agent_id
    #  Do 10 requests, waiting each time for a response
    for request in range(10):
        print("Sending pulse: %s" % request)
        message = connection.send(_pulse)
        print("Received pulse reply %s [ %s ]" % (request, message))
        sleep(1)


if __name__ == '__main__':
    main()
