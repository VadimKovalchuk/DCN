import json
import asyncio
from collections import deque

import zmq

from common.connection import get_server_socket


agents = []
clients = []
tasks = deque([])


async def server_loop():
    socket, poller = await get_server_socket('tcp://*', '9999')
    while True:
        sockets = dict(await poller.poll(1000))
        print("waiting")
        if sockets.get(socket, None) == zmq.POLLIN:
            #  Wait for next request from client
            #  await socket.recv_json()
            message = await socket.recv()
            print("Received request: %s" % message)
            #  Do some 'work'
            #  Send reply back to client
            await socket.send(b"World")


async def socket_out_loop():
    await asyncio.sleep(1)


async def start():
    _server = asyncio.create_task(server_loop())
    await _server


def main():
    asyncio.run(start())


if __name__ == '__main__':
    main()
