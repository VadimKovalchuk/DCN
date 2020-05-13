import logging
from copy import deepcopy

import pytest

from common.connection import RequestConnection
from common.request_types import register
from dispatcher.dispatcher import Dispatcher
from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

TEST_MESSAGE = {'test': 'test'}


def dummy_request_handler(request: dict):
    logger.debug(f'dummy_request_handler: {request}')
    assert request == TEST_MESSAGE, 'Test message in request is modified ' \
                                    'between "send" and "request_handler"'
    return request


def polling_expiration(is_expired: bool):
    assert is_expired, "Dispatcher haven't got request when expected"
    return True


@pytest.fixture
def dispatcher():
    with Dispatcher(port=DISPATCHER_PORT) as dispatcher:
        yield dispatcher


def test_dispatcher_connection(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        logger.debug('Sending test message')
        request_connection.socket.send_json(TEST_MESSAGE)
        dispatcher.request_handler = dummy_request_handler
        dispatcher.listen(1, polling_expiration)
        logger.debug('Waiting for response')
        reply = request_connection.socket.recv_json()
        assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                      'between "request_handler" and "reply"'


def test_dispatcher_register(dispatcher):
    name = 'this_is_test'
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        logger.debug('Sending registration message')
        register_req = deepcopy(register)
        register_req['name'] = name
        expected_id = dispatcher._next_free_id
        request_connection.socket.send_json(register_req)
        dispatcher.listen(1, polling_expiration)
        logger.debug('Waiting for response')
        reply = request_connection.socket.recv_json()
        assert reply['id'] == expected_id, 'Wrong agent id is assigned'
        assert reply['name'] == name, 'Agent id was modified'
