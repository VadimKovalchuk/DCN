import json
import socket
from typing import Union

import zmq


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


class Connection:
    port = 9999

    def __init__(self, ip: str, port: Union[int, str, None]):
        self.context = zmq.Context()
        self.ip = ip
        self.port = str(port) if port else Connection._get_free_port()

    @staticmethod
    def _get_free_port():
        while is_port_in_use(Connection.port):
            Connection.port += 1
            if Connection.port == 65535:
                Connection.port = 1024
        free_port = Connection.port
        Connection.port += 1
        return str(free_port)

    def establish(self):
        raise NotImplementedError

    def listen(self, **kwargs):
        raise RuntimeError('Request Socket is configured for listening')

    def send(self, **kwargs):
        raise RuntimeError('Attempt to send data via Input Socket')


class RequestConnection(Connection):
    def __init__(self,
                 ip: str = 'localhost',
                 port: Union[int, str, None] = None):
        super(RequestConnection, self).__init__(ip, port)
        self.socket = self.context.socket(zmq.REQ)
        self.establish()

    def establish(self):
        address = f'tcp://{self.ip}:{self.port}'
        self.socket.connect(address)

    def send(self, message: Union[str], timeout: int = 60):  # timeout in seconds
        pass


class ReplyConnection(Connection):
    def __init__(self,
                 ip: str = '*',
                 port: Union[int, str, None] = None):
        super(ReplyConnection, self).__init__(ip, port)
        self.socket = self.context.socket(zmq.REP)
        self.establish()

    def establish(self):
        address = f'tcp://{self.ip}:{self.port}'
        print(f'Starting relay at {address}')
        self.socket.bind(address)

    def listen(self, is_json: bool = True):
        if is_json:
            return self.socket.recv_json()
        else:
            return self.socket.recv()
