import json
import socket
from typing import Union

import zmq


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


class Connection:
    """

    """
    port = 9999

    def __init__(self,
                 ip: str = '*',
                 port: Union[int, str, None] = None,
                 is_input: bool = True):
        self.is_input = is_input
        self.context = zmq.Context()
        socket_type = zmq.REP if is_input else zmq.REQ
        self.socket = self.context.socket(socket_type)
        self.ip = ip
        self.port = str(port) if port else Connection._get_free_port()
        address = f'tcp://{self.ip}:{self.port}'
        if self.is_input:
            self.socket.bind(address)
        else:
            self.socket.connect(address)

    @staticmethod
    def _get_free_port():
        while is_port_in_use(Connection.port):
            Connection.port += 1
            if Connection.port == 65535:
                Connection.port = 1024
        free_port = Connection.port
        Connection.port += 1
        return str(free_port)

    def listen(self, is_json: bool = True):
        if not self.is_input:
            raise RuntimeError('Request Socket is configured for listening')
        if is_json:
            return self.socket.recv_json()
        else:
            return self.socket.recv()

    def send(self, message: Union[str, json], timeout: int = 60):  # timeout in seconds
        if self.is_input:
            raise RuntimeError('Attempt to send data via Input Socket')

        return
