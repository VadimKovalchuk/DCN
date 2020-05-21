import logging
from pathlib import Path
import shutil

import pytest

from agent.agent import Agent
from dispatcher.dispatcher import Dispatcher
from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

log_file_formatter = None
cur_log_handler = None
cur_artifacts_path = None


def polling_expiration(is_expired: bool):
    assert is_expired, "Dispatcher haven't got request when expected"
    return True


@pytest.fixture
def dispatcher():
    with Dispatcher(port=DISPATCHER_PORT) as dispatcher:
        dispatcher._interrupt = polling_expiration
        yield dispatcher


@pytest.fixture
def agent():
    with Agent(dsp_port=DISPATCHER_PORT) as agent:
        yield agent


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
