from copy import deepcopy
import logging
from typing import Callable

from common.broker import Broker
from common.connection import RequestConnection
from common.request_types import get_client_queues

logger = logging.getLogger(__name__)


class Client:
    def __init__(self,
                 name: str = '',
                 token: str = '',
                 dsp_ip: str = 'localhost',
                 dsp_port: int = 9999):
        logger.info('Starting Client')
        self.name = name
        self.token = token
        self.socket = RequestConnection(dsp_ip, dsp_port)
        self.broker = Broker(dsp_ip)
        self.task_queue = None
        self.result_queue = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.socket.close()

    def connect(self):
        self.socket.establish()

    def get_client_queues(self, callback: Callable = None):
        request = deepcopy(get_client_queues)
        request['name'] = self.name
        request['token'] = self.token
        reply = self.socket.send(request, callback=callback)
        self.broker.output_queue = reply['task_queue']
        self.broker.input_queue = reply['result_queue']

    def push_task(self, task: dict) -> bool:
        self.broker.push(task)

    def pull_result(self) -> dict:
        return self.broker.pull()


def main():
    ...


if __name__ == '__main__':
    main()
