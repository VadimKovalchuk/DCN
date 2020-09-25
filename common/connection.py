import abc
import logging
import socket
from typing import Union, Callable

import zmq

from common.defaults import DISPATCHER_PORT

logger = logging.getLogger(__name__)


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


class Connection:
    port = DISPATCHER_PORT

    def __init__(self, ip: str, port: Union[int, str, None]):
        self.context = zmq.Context()
        self.ip = ip
        self.port = str(port) if port else Connection._get_free_port()
        self.socket = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        logger.info(f'Closing {self}')
        self.socket.close()

    @staticmethod
    def _get_free_port():
        while is_port_in_use(Connection.port):
            Connection.port += 1
            if Connection.port == 65535:
                Connection.port = 1024
        free_port = Connection.port
        Connection.port += 1
        return str(free_port)

    @abc.abstractmethod
    def establish(self):
        ...

    def listen(self, **kwargs):
        raise RuntimeError('Request Socket is configured for listening')

    def send(self, **kwargs):
        raise RuntimeError('Attempt to send data via Input Socket')

    @abc.abstractmethod
    def __str__(self):
        ...


class RequestConnection(Connection):
    def __init__(self,
                 ip: str = 'localhost',
                 port: Union[int, str] = ''):
        super(RequestConnection, self).__init__(ip, port)
        self.socket = self.context.socket(zmq.REQ)

    def establish(self):
        address = f'tcp://{self.ip}:{self.port}'
        logger.info(f'Establishing connection to: {address}')
        self.socket.connect(address)

    def send(self, message: dict, timeout: int = 30,  # timeout in seconds
             callback: Callable = None):
        self.socket.send_json(message)
        if callback:
            callback()
        if self.socket.poll(timeout * 1000):
            return self.socket.recv_json()
        else:
            raise TimeoutError('No reply for Agent request')

    def close(self):
        logger.info(f'Closing {self}')
        self.socket.close()

    def __str__(self):
        return f'RequestConnection({self.ip}:{self.port})'


class ReplyConnection(Connection):
    def __init__(self,
                 ip: str = '*',
                 port: Union[int, str] = ''):
        super(ReplyConnection, self).__init__(ip, port)
        self.socket = self.context.socket(zmq.REP)

    def establish(self):
        address = f'tcp://{self.ip}:{self.port}'
        logger.info(f'Binding port for listening: {address}')
        self.socket.bind(address)

    def listen(self, request_handler: Callable, timeout: int = 30):
        if self.socket.poll(timeout * 1000):
            request = self.socket.recv_json()
            self.socket.send_json(request_handler(request))
            return True
        else:
            return False

    def close(self):
        logger.info(f'Closing {self}')
        self.socket.close()

    def __str__(self):
        return f'ReplyConnection({self.ip}:{self.port})'
