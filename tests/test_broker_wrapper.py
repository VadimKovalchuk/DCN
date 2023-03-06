import logging

from random import random
from dcn.common.broker import Broker, Task
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


def test_broker_smoke():
    client = Broker(routing_key=RoutingKeys.TASK)
    client.connect()
    logger.info('Sending task')
    for i in test_tasks:
        client.publish(test_tasks[i])
    client.close()
    agent = Broker(queue=RoutingKeys.TASK)
    agent.connect()
    logger.info('Processing task')
    for task in test_tasks:
        result = process_task(agent.consume())
        agent.publish(message=result, routing_key=RoutingKeys.RESULTS)
    agent.close()
    validator = Broker(queue=RoutingKeys.RESULTS)
    validator.connect()
    logger.info('Validating results')
    for task in test_tasks:
        validator_callback(validator.consume())
    validator.close()
