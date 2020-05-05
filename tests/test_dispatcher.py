import logging
from copy import deepcopy
from dispatcher.dispatcher import Dispatcher
from common.connection import ReplyConnection, RequestConnection

TEST_MESSAGE = {'test': 'test'}


def dummy_request_handler(request: dict):
    print(f'validating {request}')
    assert request == TEST_MESSAGE, 'Test message in request is modified ' \
                                    'between "send" and "request_handler"'
    return request


def test_dispatcher_connect():
    print('Init Dispatcher')
    dispatcher = Dispatcher()
    print('upd handler')
    dispatcher.request_handler = dummy_request_handler
    dispatcher._listen_count = 1
    print('init connection')
    request_connection = RequestConnection(port=9999)
    print('sending')
    request_connection.socket.send_json(TEST_MESSAGE)
    print('listening')
    dispatcher.listen()
    print('receiving')
    reply = request_connection.socket.recv_json()
    print(f'validating {reply}')
    assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                  'between "request_handler" and "reply"'
    print('done')


#test_dispatcher_connect()
