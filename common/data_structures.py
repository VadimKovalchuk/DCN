from copy import deepcopy

from common.constants import EXCHANGE, QUEUE
from common.defaults import EXCHANGE_NAME

queue_template = {
    EXCHANGE: EXCHANGE_NAME,
    QUEUE: ''
}


def compose_queue(name: str):
    queue = deepcopy(queue_template)
    queue[QUEUE] = name
    return queue


task_body = {
    'id': 0,
    'client': 'flush',
    'module': 'builtin',
    'function': 'dummy',
    'arguments': None
}
