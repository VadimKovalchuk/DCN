import logging

import pytest

from copy import deepcopy
from itertools import cycle
from random import random
from time import sleep
from typing import Union

from dcn.agent.agent import Agent, TaskRunner
from dcn.client.client import Client
from dcn.common.data_structures import compose_queue, task_body
from dcn.common.defaults import RoutingKeys
from dcn.dispatcher.dispatcher import Dispatcher
from tests.settings import AGENT_TEST_TOKEN, DISPATCHER_PORT

logger = logging.getLogger(__name__)

logging.getLogger('broker').setLevel(logging.WARNING)

task_input_queue = compose_queue(RoutingKeys.TASK)
task_result_queue = compose_queue(RoutingKeys.RESULTS)
test_tasks = {i: {'id': i, 'task': random()} for i in range(10)}
expected_task_sequence = {
    'consume': range(10),
    'pull': (0, 2, 4, 1, 3, 5, 6, 7, 8, 9)
}


def create_agent(name: Union[str, int]) -> Agent:
    agent = Agent(token=AGENT_TEST_TOKEN, dsp_port=DISPATCHER_PORT)
    agent.name = str(name)
    agent.socket.establish()
    agent.register()
    agent.request_broker_data()
    agent.broker.connect()
    agent.broker.inactivity_timeout = 1
    return agent


@pytest.mark.parametrize("get_task_type", ['consume', 'pull'])
def test_tasks_distribution(dispatcher: Dispatcher, client: Client, get_task_type):
    client.name = 'test'
    all((
        client.get_client_queues(),
        client.broker.connect()
    ))
    logger.info(f'{len([client.broker.publish(task) for _,task in test_tasks.items()])}'
                ' tasks generated')
    agents = [create_agent(name) for name in range(3)]
    sleep(0.1)
    for agent, exp_id in zip(cycle(agents), expected_task_sequence[get_task_type]):
        logger.debug(f'{agent} expects task {exp_id}')
        if get_task_type == 'consume':
            _, task = agent.broker.consume()
        elif get_task_type == 'pull':
            task_queue = agent.broker.pull()
            _, task = next(task_queue)
        logger.debug(task)
        assert exp_id == task['id'], 'Wrong task is received'
        # logger.debug(f'{agent} actual task {task["id"]}')
    for agent in agents:
        agent.close()


@pytest.mark.parametrize("get_task_type", ['consume', 'pull'])
def test_full_chain(agent_on_dispatcher: Agent, client_on_dispatcher: Client, get_task_type):
    agent = agent_on_dispatcher
    client = client_on_dispatcher
    # Send task from client
    test_task = deepcopy(task_body)
    test_task['client'] = client.broker.queue
    test_task['arguments'] = {'test_arg_1': 'test_val_1',
                              'test_arg_2': 'test_val_2'}
    client.broker.publish(test_task)
    # Processing task on agent
    if get_task_type == 'consume':
        sleep(0.1)
        _, task = agent.broker.consume()
    elif get_task_type == 'pull':
        task_queue = agent.broker.pull()
        _, task = next(task_queue)
    logger.debug(task)
    runner = TaskRunner(task)
    assert runner.run(), 'Error occur during task execution'
    report = runner.report
    # Validating result on client
    agent.broker.publish(report, report['client'])
    if get_task_type == 'consume':
        sleep(0.1)
        _, result = client.broker.consume()
    elif get_task_type == 'pull':
        result_queue = client.broker.pull()
        _, result = next(result_queue)
    assert test_task['arguments'] == result['result'], \
        'Wrong task is received from task queue for Agent'
