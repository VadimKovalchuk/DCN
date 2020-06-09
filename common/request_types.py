class Commands:
    register = 'register'
    pulse = 'pulse'
    client_queues = 'client_queues'


register = {
    'id': 0,
    'command': Commands.register,
    'type': 'unified',
    'name': '',
    'broker': {
        'host': '',
        'task': '',
        'result': ''
    },
    'result': False
}

pulse = {
    'id': 0,
    'command': Commands.pulse,
    'reply': {}
}

client_queues = {
    'command': Commands.client_queues,
    'name': '',
    'token': '',
    'broker': '',
    'task_queue': '',
    'result_queue': ''
}

task = {
    'module': 'builtin',
    'function': 'dummy',
    'arguments': None
}

task_report = {}

