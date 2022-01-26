import logging

from random import random
from common.broker import Broker, Task
from common.data_structures import compose_queue
from common.defaults import RoutingKeys

logger = logging.getLogger(__name__)

task_input_queue = compose_queue(RoutingKeys.TASK)
task_result_queue = compose_queue(RoutingKeys.RESULTS)
test_tasks = {i: {'id': i, 'task': random()} for i in range(10)}


def process_task(task: Task):
    assert test_tasks[task.body['id']] == task.body, 'Task differs from expected one'
    return {'id': task.body['id'], 'result': task.body['task']}


def validator_callback(task: Task):
    assert test_tasks[task.body['id']]['task'] == task.body['result'], 'Task differs from expected one'


def test_broker_smoke():
    with Broker('localhost') as client:
        client.connect()
        client.setup_exchange()
        client.output_queue = task_input_queue
        logger.info('Sending task')
        for i in test_tasks:
            client.push(test_tasks[i])
    with Broker('localhost') as agent:
        agent.connect()
        agent.declare(input_queue=task_input_queue)
        agent.output_queue = task_result_queue
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
