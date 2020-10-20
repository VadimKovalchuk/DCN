from copy import deepcopy
from functools import partial
import logging

from agent.agent import Agent
from dispatcher.dispatcher import Dispatcher
from common.broker import Broker
from common.data_structures import compose_queue, task_body
from common.defaults import RoutingKeys

logger = logging.getLogger(__name__)


def test_agent_registration(dispatcher: Dispatcher, agent: Agent):
    name = 'agent_test_name'
    agent.name = name
    interrupt = partial(dispatcher.listen, 1)
    agent.register(interrupt)
    assert agent.id in dispatcher.agents, 'Agent ID mismatch'
    assert agent.name == dispatcher.agents[agent.id].name, \
        'Agent name mismatch'
    assert 0.01 > (agent.last_sync - dispatcher.agents[agent.id].last_sync).seconds,\
        'Request-Reply sync timestamp differs more than expected'


def test_agent_pulse(dispatcher: Dispatcher, agent: Agent):
    interrupt = partial(dispatcher.listen, 1)
    agent.register(interrupt)
    for _ in range(10):
        assert agent.pulse(interrupt), 'Wrong reply status'


def test_agent_queues(agent_on_dispatcher: Agent, broker: Broker):
    agent = agent_on_dispatcher
    test_task = deepcopy(task_body)
    test_task['arguments'] = 'agent_task_test'
    task_result = deepcopy(task_body)
    task_result['arguments'] = 'agent_task_result'
    broker.declare(input_queue=compose_queue(RoutingKeys.RESULTS),
                   output_queue=compose_queue(RoutingKeys.TASK))
    broker.push(test_task)
    task = next(agent.broker.pulling_generator())
    agent.broker.set_task_done(task)
    assert test_task == task.body, \
        'Wrong task is received from task queue for Agent'
    agent.broker.push(task_result)
    result = next(broker.pulling_generator())
    broker.set_task_done(result)
    assert task_result == result.body, \
        'Wrong Agent result is received from task queue'
