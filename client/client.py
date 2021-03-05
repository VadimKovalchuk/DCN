from copy import deepcopy
import logging
from typing import Callable

from common.broker import Broker
from common.connection import RequestConnection
from common.request_types import Client_queues

logger = logging.getLogger(__name__)


class Client:
    def __init__(self,
                 name: str,
                 token: str,
                 dsp_ip: str = 'localhost',
                 dsp_port: int = 9999):
        logger.info('Starting Client')
        self.name = name
        self.token = token
        self.socket = RequestConnection(dsp_ip, dsp_port)
        self.broker = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.socket.close()

    def connect(self):
        self.socket.establish()

    def get_client_queues(self, callback: Callable = None):
        request = deepcopy(Client_queues)
        request['name'] = self.name
        request['token'] = self.token
        reply = self.socket.send(request, callback=callback)
        if reply['result']:
            self.broker = Broker(reply['broker']['host'])
            self.broker.connect()
            self.broker.declare(reply['broker']['result'], reply['broker']['task'])
        else:
            ConnectionRefusedError('Invalid credentials or resource is busy')


def main():
    ...


if __name__ == '__main__':
    main()
