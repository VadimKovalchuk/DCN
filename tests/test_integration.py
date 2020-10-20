import logging

from functools import partial
from itertools import cycle
from random import random
from typing import Callable, Union

from agent.agent import Agent
from client.client import Client
from common.broker import Task
from common.data_structures import compose_queue
from common.defaults import RoutingKeys
from dispatcher.dispatcher import Dispatcher
from tests.settings import DISPATCHER_PORT

logger = logging.getLogger(__name__)

task_input_queue = compose_queue(RoutingKeys.TASK)
task_result_queue = compose_queue(RoutingKeys.RESULTS)
test_tasks = {i: {'id': i, 'task': random()} for i in range(10)}
expected_task_sequence = (0, 2, 4, 1, 3, 5, 6, 7, 8, 9)


def create_agent(name: Union[str, int], interrupt: Callable) -> Agent:
    agent = Agent(dsp_port=DISPATCHER_PORT)
    agent.name = str(name)
    agent.connect()
    agent.register(interrupt)
    agent.broker._inactivity_timeout = 0.1
    return agent


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
