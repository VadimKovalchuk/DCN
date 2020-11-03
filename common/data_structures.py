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
    'client': compose_queue('flush'),
    'module': 'builtin',
    'function': 'relay',
    'arguments': None
}


task_report = {
    'id': 0,
    'client': compose_queue('flush'),
    'result': '',
    'status': False,
    'resolution': 'Unhandled error',
}
