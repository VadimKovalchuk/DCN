from copy import deepcopy
from functools import partial
import logging

from common.request_types import Task

logger = logging.getLogger(__name__)


def test_agent_registration(dispatcher, agent):
    name = 'agent_test_name'
    agent.name = name
    interrupt = partial(dispatcher.listen, 1)
    agent.register(interrupt)
    assert agent.id in dispatcher.agents, 'Agent ID mismatch'
    assert agent.name == dispatcher.agents[agent.id].name, 'Agent name mismatch'
    assert 0.01 > (agent.last_sync - dispatcher.agents[agent.id].last_sync).seconds,\
        'Request-Reply sync timestamp differs more than expected'


def test_agent_pulse(dispatcher, agent):
    interrupt = partial(dispatcher.listen, 1)
    agent.register(interrupt)
    for _ in range(10):
        assert agent.pulse(interrupt), 'Wrong reply status'


def test_agent_queues(dispatcher, agent):
    test_task = deepcopy(Task)
    test_task['arguments'] = 'agent_task_test'
    dispatcher.broker.push(test_task)
    interrupt = partial(dispatcher.listen, 1)
    agent.register(interrupt)
    queue = agent.broker.pulling_generator()
    task = next(queue)
    agent.broker.set_task_done(task)
    logger.debug(task.body)
    assert test_task == task.body, 'Wrong task is received from task queue'
