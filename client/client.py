from copy import deepcopy
import logging
from time import sleep
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
        self.socket.establish()
        return self

    def __exit__(self, *exc_info):
        if self.broker:
            self.broker.close()
        self.socket.close()

    def get_client_queues(self):
        request = deepcopy(Client_queues)
        request['name'] = self.name
        request['token'] = self.token
        reply = self.socket.send(request)
        if reply['result']:
            self.broker = Broker(reply['broker']['host'])
            if self.broker.connect():
                self.broker.declare(reply['broker']['result'], reply['broker']['task'])
        else:
            ConnectionRefusedError('Invalid credentials or resource is busy')

    def ensure_broker_connection(self, delay: int = 10, retry_count: int = 30):
        _try = 0
        while not self.broker.connect():
            if _try == retry_count:
                return False
            sleep(delay)
        logger.info('Broker connection reached')
        for _ in range(5):
            if not self.broker.connected:
                return False
            sleep(1)
        logger.info('Broker connection is stable')
        return True


def main():
    ...


if __name__ == '__main__':
    main()
