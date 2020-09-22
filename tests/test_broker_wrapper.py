import logging

from functools import partial
from itertools import cycle
from random import random
from typing import Callable, Union

from agent.agent import Agent
from client.client import Client
from common.broker import Broker, Task
from dispatcher.dispatcher import Dispatcher
from tests.conftest import flush_queue
from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

task_input_queue = 'test_task'
task_result_queue = 'test_result'
test_tasks = {i: {'id': i, 'task': random()} for i in range(10)}
expected_task_sequence = (0, 2, 4, 1, 3, 5, 6, 7, 8, 9)


def process_task(task: Task):
    assert test_tasks[task.body['id']] == task.body, 'Task differs from expected one'
    return {'id': task.body['id'], 'result': task.body['task']}


def validator_callback(task: Task):
    assert test_tasks[task.body['id']]['task'] == task.body['result'], 'Task differs from expected one'


def create_agent(name: Union[str, int], interrupt: Callable) -> Agent:
    agent = Agent(dsp_port=DISPATCHER_PORT)
    agent.name = str(name)
    agent.connect()
    agent.register(interrupt)
    agent.broker._inactivity_timeout = 0.1
    return agent


def test_broker_smoke():
    with Broker('localhost') as client:
        client.connect()
        client.declare(output_queue=task_input_queue)
        logger.info('Sending task')
        for i in test_tasks:
            client.push(test_tasks[i])
    with Broker('localhost') as agent:
        agent.connect()
        agent.declare(input_queue=task_input_queue,
                      output_queue=task_result_queue)
        agent._inactivity_timeout = 0.1
        logger.info('Processing task')
        for task in agent.pulling_generator():
            result = process_task(task)
            agent.set_task_done(task)
            agent.push(result)
    with Broker('localhost') as validator:
        validator.connect()
        validator.declare(input_queue=task_result_queue)
        validator._inactivity_timeout = 0.1
        logger.info('Validating results')
        for task in validator.pulling_generator():
            validator_callback(task)
            validator.set_task_done(task)


def test_tasks_distribution(dispatcher: Dispatcher, client: Client):
    interrupt = partial(dispatcher.listen, 1)
    client.name = 'test'
    client.get_client_queues(interrupt)
    logger.info(f'{len([client.broker.push(task) for _,task in test_tasks.items()])}'
                ' tasks generated')
    agents = [create_agent(name, interrupt) for name in range(3)]
    for agent, exp_id in zip(cycle(agents), expected_task_sequence):
        logger.debug(f'{agent} expects task {exp_id}')
        task_queue = agent.broker.pulling_generator()
        task = next(task_queue)
        assert exp_id == task.body['id'], 'Wrong task is received'
        agent.broker.set_task_done(task)
    for agent in agents:
        agent.close()
