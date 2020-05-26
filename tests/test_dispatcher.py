import logging
from copy import deepcopy
from datetime import datetime

from common.connection import RequestConnection
from common.request_types import register, pulse

from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

TEST_MESSAGE = {'test': 'test'}


def dummy_request_handler(request: dict):
    logger.info(f'dummy_request_handler: {request}')
    assert request == TEST_MESSAGE, 'Test message in request is modified ' \
                                    'between "send" and "request_handler"'
    return request


def test_dispatcher_agent_connection(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        logger.info('Sending test message')
        request_connection.socket.send_json(TEST_MESSAGE)
        logger.info('Setting handler')
        dispatcher.request_handler = dummy_request_handler
        logger.info('call listen')
        dispatcher.listen(1)
        logger.info('Waiting for response')
        reply = request_connection.socket.recv_json()
        assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                      'between "request_handler" and "reply"'


def test_dispatcher_register(dispatcher):
    name = 'this_is_test'
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        request_connection.establish()
        logger.info('Sending registration message')
        register_req = deepcopy(register)
        register_req['name'] = name
        expected_id = dispatcher._next_free_id
        request_connection.socket.send_json(register_req)
        dispatcher.listen(1)
        logger.info('Waiting for response')
        reply = request_connection.socket.recv_json()
        assert reply['result'], 'Registration was not successful'
        assert reply['id'] == expected_id, 'Wrong agent id is assigned'
        assert expected_id in dispatcher.agents, 'Agent is missing in ' \
                                                 'dispatcher agents list'
        assert dispatcher.agents[expected_id].name == name, 'Agent name does not ' \
            'corresponds to expected one'
        now = datetime.utcnow()
        last_sync = dispatcher.agents[expected_id].last_sync
        assert 0.01 > (now - last_sync).seconds, 'Request-Reply sync timestamp' \
                                                 'differs more than expected'


def test_dispatcher_pulse(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        register_req = deepcopy(register)
        request_connection.socket.send_json(register_req)
        dispatcher.listen(1)
        reply = request_connection.socket.recv_json()
        for _ in range(10):
            pulse_req = deepcopy(pulse)
            pulse_req['id'] = reply['id']
            request_connection.socket.send_json(pulse_req)
            dispatcher.listen(1)
            reply = request_connection.socket.recv_json()
            assert 'ok' == reply['reply']['status'], 'Wrong reply status'
