from copy import deepcopy
from functools import partial
import logging

from dispatcher.dispatcher import Dispatcher
from common.broker import Broker
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


def test_agent_queues(dispatcher, agent, broker):
    test_task = deepcopy(Task)
    test_task['arguments'] = 'agent_task_test'
    task_result = deepcopy(Task)
    task_result['arguments'] = 'agent_task_result'
    interrupt = partial(dispatcher.listen, 1)
    broker.declare(input_queue='result', output_queue='task')
    broker.push(test_task)
    agent.register(interrupt)
    task = next(agent.broker.pulling_generator())
    agent.broker.set_task_done(task)
    assert test_task == task.body, 'Wrong task is received from task queue for Agent'
    agent.broker.push(task_result)
    result = next(broker.pulling_generator())
    broker.set_task_done(result)
    assert task_result == result.body, 'Wrong Agent result is received from task queue'
