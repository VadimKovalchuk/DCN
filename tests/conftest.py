import logging
import shutil

from pathlib import Path
from typing import Union

import pytest

from agent.agent import Agent
from client.client import Client
from common.broker import Broker
from dispatcher.dispatcher import Dispatcher
from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

log_file_formatter = None
cur_log_handler = None
cur_artifacts_path = None


def flush_queue(broker: str,
                queue: str = 'task',
                assert_non_empty: bool = True):
    with Broker(broker) as br:
        br.connect()
        br.declare(queue)
        br._inactivity_timeout = 0.1  # seconds
        empty = True
        logger.info(f'Flushing queue: {queue}')
        for task in br.pulling_generator():
            empty = False
            br.set_task_done(task)
        if assert_non_empty:
            assert empty, f'Flushed queue {queue} is not empty'


def polling_expiration(is_expired: bool):
    assert is_expired, "Dispatcher haven't got request when expected"
    return True


@pytest.fixture
def broker():
    host = '*'
    input_queue = 'task'
    with Broker(host) as broker:
        broker.connect()
        broker._interrupt = polling_expiration
        broker._inactivity_timeout = 0.1  # seconds
        broker.declare(input_queue=input_queue, output_queue='result')
        yield broker
        input_queue = broker.input_queue
    flush_queue(host, input_queue)


@pytest.fixture
def dispatcher():
    with Dispatcher(port=DISPATCHER_PORT) as dispatcher:
        dispatcher.connect()
        dispatcher._interrupt = polling_expiration
        dispatcher.broker._inactivity_timeout = 0.1  # seconds
        flush_queue(dispatcher.broker.host, assert_non_empty=False)
        yield dispatcher
        flush_queue(dispatcher.broker.host)


@pytest.fixture
def agent():
    with Agent(dsp_port=DISPATCHER_PORT) as agent:
        agent.connect()
        yield agent
        if agent.broker and agent.broker.input_queue:
            flush_queue(agent.broker.host, agent.broker.input_queue)


@pytest.fixture
def client():
    with Client(dsp_port=DISPATCHER_PORT) as client:
        client.connect()
        yield client
        flush_queue(client.broker.host, client.broker.input_queue)


# PYTEST HOOKS
def pytest_sessionstart(session):
    global log_file_formatter
    log_file_formatter = logging.Formatter(
        session.config.getini('log_file_format'),
        session.config.getini('log_file_date_format'))


def pytest_runtest_logstart(nodeid, location):
    global cur_log_handler, cur_artifacts_path
    filename, linenum, testname = location

    testname = testname.replace('/', '_')
    cur_artifacts_path = Path('log', testname)
    if cur_artifacts_path.is_dir():
        shutil.rmtree(cur_artifacts_path)
    cur_artifacts_path.mkdir(exist_ok=True, parents=True)

    cur_log_handler = logging.FileHandler(
        cur_artifacts_path / 'pytest.log.txt', mode='w')
    cur_log_handler.setLevel(logging.DEBUG)
    cur_log_handler.setFormatter(log_file_formatter)

    logging.getLogger().addHandler(cur_log_handler)

    logger.info(f'Start test: {nodeid}')
