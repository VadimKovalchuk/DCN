import logging
from copy import deepcopy
from dispatcher.dispatcher import Dispatcher
from common.connection import RequestConnection

logger = logging.getLogger(__name__)

TEST_MESSAGE = {'test': 'test'}


def dummy_request_handler(request: dict):
    logger.debug(f'dummy_request_handler: {request}')
    assert request == TEST_MESSAGE, 'Test message in request is modified ' \
                                    'between "send" and "request_handler"'
    return request


def test_dispatcher_connection():
    dispatcher = Dispatcher()
    request_connection = RequestConnection(port=9999)
    logger.debug('Sending test message')
    request_connection.socket.send_json(TEST_MESSAGE)
    dispatcher.connection.listen(dummy_request_handler)
    logger.debug('Waiting for response')
    reply = request_connection.socket.recv_json()
    assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                  'between "request_handler" and "reply"'
    dispatcher.connection.close()


def test_dispatcher_register():
    logger.debug('Starting Dispatcher')
    dispatcher = Dispatcher(port=9999)
    request_connection = RequestConnection(port=9999)
    logger.debug('Sending test message')
    request_connection.socket.send_json(TEST_MESSAGE)
    dispatcher.connection.listen(dummy_request_handler)
    logger.debug('Waiting for response')
    reply = request_connection.socket.recv_json()
    assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                  'between "request_handler" and "reply"'
    dispatcher.connection.close()

