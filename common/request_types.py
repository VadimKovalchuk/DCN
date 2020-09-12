class Commands:
    Register = 'register'
    Pulse = 'pulse'
    Client_queues = 'client_queues'


Register = {
    'id': 0,
    'command': Commands.Register,
    'type': 'unified',
    'name': '',
    'broker': {
        'host': '',
        'task': '',
        'result': ''
    },
    'result': False
}

Pulse = {
    'id': 0,
    'command': Commands.Pulse,
    'reply': {}
}

Client_queues = {
    'command': Commands.Client_queues,
    'token': '',
    'broker': '',
    'task_queue': '',
    'result_queue': ''
}

Task = {
    'module': 'builtin',
    'function': 'dummy',
    'arguments': None
}

Task_report = {}

