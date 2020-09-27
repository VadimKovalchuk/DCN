from copy import deepcopy
from functools import partial
import logging

from common.data_structures import compose_queue
from common.defaults import RoutingKeys
from common.request_types import Task

logger = logging.getLogger(__name__)


def test_client_initialization(dispatcher, client):
    client.name = 'client_test_name'
    interrupt = partial(dispatcher.listen, 1)
    client.get_client_queues(interrupt)
    assert client.broker, 'Broker instance is missing'
    assert client.broker.output_queue == compose_queue(RoutingKeys.TASK), \
        f'Output queue is not {RoutingKeys.TASK}'
    assert client.broker.input_queue == compose_queue(client.name), \
        'Input queue does not corresponds to client name ' \
        f'{client.name}'


def test_client_queues(dispatcher, client, broker):
    name = 'client_test_name'
    client.name = name
    interrupt = partial(dispatcher.listen, 1)
    client.get_client_queues(interrupt)
    test_task = deepcopy(Task)
    test_task['arguments'] = 'client_task_test'
    client.broker.push(test_task)
    queue = broker.pulling_generator()
    task = next(queue)
    broker.set_task_done(task)
    assert task.body['arguments'] == 'client_task_test', \
        'Client task name mismatch'
    answer = task.body
    answer['arguments'] = 'client_task_result'
    broker.declare(output_queue=compose_queue(client.name))
    broker.push(answer)
    client_task_queue = client.broker.pulling_generator()
    result = next(client_task_queue)
    client.broker.set_task_done(result)
    assert result.body['arguments'] == 'client_task_result', \
        'Client result name mismatch'
