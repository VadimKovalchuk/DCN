from copy import deepcopy
from functools import partial
import logging

from common.request_types import Task

logger = logging.getLogger(__name__)


def test_agent_initialization(dispatcher, client):
    broker = dispatcher.broker
    broker._inactivity_timeout = 0.1
    name = 'client_test_name'
    client.name = name
    interrupt = partial(dispatcher.listen, 1)
    client.get_client_queues(interrupt)
    test_task = deepcopy(Task)
    test_task['arguments'] = 'client_task_test'
    client.push_task(test_task)
    task_stub = {}
    broker_task_queue = broker.pull()
    for _, _, task in broker.pull():
        task_stub = task
    assert task_stub['arguments'] == 'client_task_test', \
        'Client task name mismatch'
    task_stub['arguments'] = 'client_task_result'
    broker.declare(output_queue=client.name)
    broker.push(task_stub)
    client_task_queue = client.pull_result()
    (_, _, result) = next(client_task_queue)
    assert result['arguments'] == 'client_task_result', \
        'Client result name mismatch'
