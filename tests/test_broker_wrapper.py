import logging
from random import random

import pytest

from dcn.common.broker import Broker
from dcn.common.data_structures import compose_queue
from dcn.common.defaults import RoutingKeys

logger = logging.getLogger(__name__)

task_input_queue = compose_queue(RoutingKeys.TASK)
task_result_queue = compose_queue(RoutingKeys.RESULTS)
test_tasks = {i: {'id': i, 'task': random()} for i in range(10)}


def process_task(task):
    status, message = task
    assert status, f'Failed to process task status: {task}'
    if message:
        assert test_tasks[message['id']] == message, 'Task differs from expected one'
        return {'id': message['id'], 'result': message['task']}


def validator_callback(task):
    status, message = task
    assert status, f'Failed to process task status: {task}'
    if message:
        assert test_tasks[message['id']]['task'] == message['result'], 'Task differs from expected one'


@pytest.mark.parametrize("get_task_type", ['consume', 'pull'])
def test_broker_smoke(get_task_type):
    client = Broker()
    client.output_routing_key = RoutingKeys.TASK
    client.connect()
    logger.info('Sending task')
    for i in test_tasks:
        client.publish(test_tasks[i])
    client.close()
    agent = Broker(queue=RoutingKeys.TASK)
    agent.connect()
    logger.info('Processing task')
    for _ in test_tasks:
        if get_task_type == 'consume':
            task = agent.consume()
        elif get_task_type == 'pull':
            task_queue = agent.pull()
            task = next(task_queue)
        result = process_task(task)
        agent.publish(message=result, routing_key=RoutingKeys.RESULTS)
    agent.close()
    validator = Broker(queue=RoutingKeys.RESULTS)
    validator.connect()
    logger.info('Validating results')
    for _ in test_tasks:
        if get_task_type == 'consume':
            task = validator.consume()
        elif get_task_type == 'pull':
            task_queue = validator.pull()
            task = next(task_queue)
        validator_callback(task)
    validator.close()
