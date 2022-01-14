import logging
import shutil

from pathlib import Path
from threading import Thread
from time import sleep

import docker
import pytest

from agent.agent import Agent
from client.client import Client
from common.broker import Broker
from common.constants import SECOND
from common.data_structures import compose_queue
from common.defaults import RoutingKeys
from dispatcher.dispatcher import Dispatcher
from tests.settings import AGENT_TEST_TOKEN, CLIENT_TEST_TOKEN,\
    DISPATCHER_PORT

logger = logging.getLogger(__name__)


BROKER_HOST = '*'
DISPATCHER_LISTEN_TIMEOUT = 0.01

log_file_formatter = None
cur_log_handler = None
cur_artifacts_path = None

task_queue = compose_queue(RoutingKeys.TASK)


def flush_queue(broker: str,
                queue: dict = task_queue,
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


@pytest.fixture(autouse=True, scope='session')
def rabbit_mq():
    client = docker.from_env()
    containers_list = client.containers.list(filters={'name': 'rabbitmq'})
    logger.info([container.name for container in containers_list])
    if containers_list:
        container = containers_list[0]
    else:
        container = client.containers.run(
            image='rabbitmq:3.9.11-management',
            auto_remove=True,
            detach=True,
            name='rabbitmq',
            ports={
                '5672': '5672',
                '15672': '15672'
            }
        )
    with Broker(BROKER_HOST) as broker:
        while not broker.connect():
            sleep(10)
    yield container


@pytest.fixture
def broker():
    with Broker(BROKER_HOST) as broker:
        while not broker.connect():
            sleep(10)
        broker._inactivity_timeout = 0.1 * SECOND
        broker.declare(input_queue=task_queue,
                       output_queue=compose_queue(RoutingKeys.RESULTS))
        yield broker
        input_queue = broker.input_queue
    flush_queue(BROKER_HOST, input_queue)


@pytest.fixture
def dispatcher():
    with Dispatcher(port=DISPATCHER_PORT) as dispatcher:
        # dispatcher.connect()
        dispatcher.broker._inactivity_timeout = 0.1 * SECOND
        flush_queue(dispatcher.broker.host, assert_non_empty=False)
        listener = Thread(target=dispatcher.listen, args=[DISPATCHER_LISTEN_TIMEOUT])
        listener.start()
        yield dispatcher
        dispatcher._listen = False
        sleep(0.02)  # wait while dispatcher listener is closed
        logger.info(f'Dispatcher listener state: {listener.is_alive()}')
        flush_queue(dispatcher.broker.host)


@pytest.fixture
def agent():
    with Agent(token=AGENT_TEST_TOKEN, dsp_port=DISPATCHER_PORT) as agent:
        yield agent
        if agent.broker and agent.broker.input_queue:
            flush_queue(agent.broker.host, agent.broker.input_queue)


@pytest.fixture()
def agent_on_dispatcher(dispatcher: Dispatcher, agent: Agent):
    agent.register()
    agent.request_broker_data()
    agent.broker._inactivity_timeout = 0.1 * SECOND
    yield agent


@pytest.fixture
def client():
    with Client(name='test_client',
                token=CLIENT_TEST_TOKEN,
                dsp_port=DISPATCHER_PORT) as client:
        yield client
        if client.broker:
            host = client.broker.host
            input_queue = client.broker.input_queue
        else:
            return
    flush_queue(host, input_queue)


@pytest.fixture()
def client_on_dispatcher(client: Client, dispatcher: Dispatcher):
    client.get_client_queues()
    client.broker._inactivity_timeout = 0.1 * SECOND
    yield client


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
