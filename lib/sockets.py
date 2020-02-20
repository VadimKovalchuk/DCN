import asyncio

import zmq
import zmq.asyncio


context = zmq.asyncio.Context()


async def get_server_socket(host: str, port: str):
    """
    Get ZeroMQ asyncio socket.

    :param host: Server hostname or IP address
    :param port: Server receiving port
    :return: zmq.asyncio.socket
    """
    socket = context.socket(zmq.REP)
    socket.bind(f'{host}:{port}')  # tcp://localhost:5555
    poller = zmq.asyncio.Poller()
    poller.register(socket, zmq.POLLIN)
    return socket, poller
