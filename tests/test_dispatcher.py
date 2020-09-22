import logging
from copy import deepcopy
from datetime import datetime
from functools import partial

from common.connection import RequestConnection
from common.request_types import Register, Pulse, Client_queues

from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

TEST_MESSAGE = {'test': 'test'}


def dummy_request_handler(request: dict):
    logger.info(f'dummy_request_handler: {request}')
    assert request == TEST_MESSAGE, 'Test message in request is modified ' \
                                    'between "send" and "request_handler"'
    return request


def test_dsp_agent_connection(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        logger.info('Sending test message')
        dispatcher.request_handler = dummy_request_handler
        callback = partial(dispatcher.listen, 1)
        reply = request_connection.send(TEST_MESSAGE, 1, callback)
        assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                      'between "request_handler" and "reply"'


def test_dsp_register(dispatcher):
    name = 'this_is_test'
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        request_connection.establish()
        logger.info('Sending registration message')
        register_req = deepcopy(Register)
        register_req['name'] = name
        expected_id = dispatcher._next_free_id
        callback = partial(dispatcher.listen, 1)
        reply = request_connection.send(register_req, 1, callback)
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


def test_dsp_pulse(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        register_req = deepcopy(Register)
        callback = partial(dispatcher.listen, 1)
        reply = request_connection.send(register_req, 1, callback)
        for _ in range(10):
            pulse_req = deepcopy(Pulse)
            pulse_req['id'] = reply['id']
            reply = request_connection.send(pulse_req, 1, callback)
            assert 'ok' == reply['reply']['status'], 'Wrong reply status'


def test_dsp_client_queue(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        request = deepcopy(Client_queues)
        request['name'] = 'test_dsp_client_queue'
        callback = partial(dispatcher.listen, 1)
        reply = request_connection.send(request, 1, callback)
        assert reply['name'] == request['name'], \
            'Name param is modified or wrong reply'
        assert reply['result_queue'] == request['name'], \
            'Wrong result queue name is defined by dispatcher'
        assert reply['task_queue'] == 'task'
