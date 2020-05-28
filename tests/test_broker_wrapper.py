import logging
from random import random

from common.broker import Broker

logger = logging.getLogger(__name__)

task_input_queue = 'test_task'
task_result_queue = 'test_result'
test_tasks = {i: {'id': i, 'task': random()} for i in range(10)}


def processing_callback(method, properties, task):
    if not all((method, properties, task)):
        return False
    else:
        assert test_tasks[task['id']] == task, 'Task differs from expected one'
        return {'id': task['id'], 'result': task['task']}


def validator_callback(method, properties, task):
    assert test_tasks[task['id']]['task'] == task['result'], 'Task differs from expected one'
    return False


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
        agent.pull(processing_callback)
    with Broker('localhost') as validator:
        validator.connect()
        validator.declare(input_queue=task_result_queue)
        validator._inactivity_timeout = 0.1
        logger.info('Validating results')
        validator.pull(validator_callback)


