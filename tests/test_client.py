from copy import deepcopy
from time import sleep
import logging

from dcn.common.data_structures import compose_queue, task_body
from dcn.common.defaults import RoutingKeys

logger = logging.getLogger(__name__)


def test_client_initialization(dispatcher, client):
    client.name = 'client_test_name'
    client.get_client_queues()
    assert client.broker, 'Broker instance is missing'
    assert client.broker.output_routing_key == RoutingKeys.TASK, \
        f'Output queue is not {RoutingKeys.TASK}'
    assert client.broker.queue == client.name, \
        'Input queue does not corresponds to client name ' \
        f'{client.name}'


def test_client_queues(dispatcher, client, broker):
    name = 'client_test_name'
    client.name = name
    assert all((
        client.get_client_queues(),
        client.broker.connect()
    )), 'Broker operability is not reached'
    test_task = deepcopy(task_body)
    test_task['arguments'] = 'client_task_test'
    client.broker.publish(test_task)
    sleep(0.1)
    _, task = broker.consume()
    assert task['arguments'] == 'client_task_test', \
        'Client task name mismatch'
    answer = task
    answer['arguments'] = 'client_task_result'
    broker.publish(answer, client.name)
    sleep(0.1)
    _, result = client.broker.consume()
    assert result['arguments'] == 'client_task_result', \
        'Client result name mismatch'
