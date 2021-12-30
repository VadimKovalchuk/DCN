import logging
from copy import deepcopy
from datetime import datetime
from functools import partial

from common.connection import RequestConnection
from common.constants import QUEUE
from common.defaults import RoutingKeys
from common.request_types import Register_agent, Pulse, Client_queues, Commands

from tests.settings import CLIENT_TEST_TOKEN, DISPATCHER_PORT

logger = logging.getLogger(__name__)

TEST_MESSAGE = {'command': Commands.Relay, 'test': 'test'}


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
        reply = request_connection.send(TEST_MESSAGE, 1)
        assert reply == TEST_MESSAGE, 'Test message in request is modified ' \
                                      'between "request_handler" and "reply"'


def test_dsp_register(dispatcher):
    name = 'this_is_test'
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        logger.info('Sending registration message')
        register_req = deepcopy(Register_agent)
        register_req['name'] = name
        register_req['token'] = CLIENT_TEST_TOKEN
        expected_id = dispatcher._next_free_id
        reply = request_connection.send(register_req, 1)
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
        register_req = deepcopy(Register_agent)
        register_req['token'] = CLIENT_TEST_TOKEN
        reply = request_connection.send(register_req, 1)
        pulse_req = deepcopy(Pulse)
        pulse_req['id'] = reply['id']
        for _ in range(10):
            reply = request_connection.send(pulse_req, 1)
            assert reply['result'], 'Wrong reply status'


def test_dsp_client_queue(dispatcher):
    with RequestConnection(port=DISPATCHER_PORT) as request_connection:
        request_connection.establish()
        request = deepcopy(Client_queues)
        request['name'] = 'test_dsp_client_queue'
        request['token'] = CLIENT_TEST_TOKEN
        reply = request_connection.send(request, 1)
        assert reply['name'] == request['name'], \
            'Name param is modified or wrong reply'
        # TODO: Validate broker host
        assert reply['broker']['result'][QUEUE] == request['name'], \
            'Wrong result queue name is defined by dispatcher'
        assert reply['broker']['task'][QUEUE] == RoutingKeys.TASK, \
            f'Task queue is not "{RoutingKeys.TASK}"'
