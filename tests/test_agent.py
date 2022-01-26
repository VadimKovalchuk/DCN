from copy import deepcopy
import logging

from agent.agent import Agent, RemoteAgent
from dispatcher.dispatcher import Dispatcher
from common.broker import Broker
from common.data_structures import compose_queue, task_body
from common.defaults import RoutingKeys
from common.request_types import Commands, Disconnect


logger = logging.getLogger(__name__)


def test_agent_registration(dispatcher: Dispatcher, agent: Agent):
    name = 'agent_test_name'
    agent.name = name
    agent.register()
    assert agent.id in dispatcher.agents, 'Agent ID mismatch'
    assert agent.name == dispatcher.agents[agent.id].name, \
        'Agent name mismatch'
    assert 0.01 > (agent.last_sync - dispatcher.agents[agent.id].last_sync).seconds,\
        'Request-Reply sync timestamp differs more than expected'


def test_agent_pulse(dispatcher: Dispatcher, agent: Agent):
    agent.register()
    for _ in range(10):
        assert agent.pulse(), 'Wrong reply status'


def test_agent_queues(agent_on_dispatcher: Agent, broker: Broker):
    agent = agent_on_dispatcher
    test_task = deepcopy(task_body)
    test_task['arguments'] = 'agent_task_test'
    task_result = deepcopy(task_body)
    task_result['arguments'] = 'agent_task_result'
    broker.output_queue = compose_queue(RoutingKeys.TASK)
    broker.declare(compose_queue(RoutingKeys.RESULTS))
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


def test_agent_command_delivery(dispatcher: Dispatcher, agent_on_dispatcher: Agent):
    agent = agent_on_dispatcher
    remote_agent: RemoteAgent = dispatcher.agents[agent.id]
    remote_agent.commands = ['test']
    agent.pulse()
    assert agent.commands == ['test'], 'Agent command delivery has failed'


def test_agent_disconnect(dispatcher: Dispatcher, agent_on_dispatcher: Agent):
    agent = agent_on_dispatcher
    assert agent.disconnect(), 'Agent Disconnect request has failed'
    assert agent.id not in dispatcher.agents, 'Agent registration is still active on Dispatcher'
    assert not agent.broker, 'Agent is not disconnected from Broker'
    assert agent.id == 0, 'Agent is not dropped'


def test_agent_connect_after_disconnect(dispatcher: Dispatcher, agent_on_dispatcher: Agent):
    agent = agent_on_dispatcher
    new_agent_id = dispatcher._next_free_id
    assert agent.disconnect(), 'Agent Disconnect request has failed'
    agent.register()
    agent.request_broker_data()
    assert agent.id == new_agent_id, f'Unexpected agent ID {agent.id} instead of {new_agent_id}'


def test_agent_command_apply(dispatcher: Dispatcher, agent_on_dispatcher: Agent):
    agent = agent_on_dispatcher
    remote_agent: RemoteAgent = dispatcher.agents[agent.id]
    remote_agent.commands = ['disconnect']
    agent.pulse()
    assert agent.apply_commands(), 'Agent command execution failure'
    assert agent.id == 0, 'Agent is not dropped'
