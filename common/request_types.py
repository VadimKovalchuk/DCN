class Commands:
    Agent_queues = 'agent_queues'
    Client_queues = 'client_queues'
    Pulse = 'pulse'
    Register_agent = 'register_agent'


Agent_queues = {
    'id': 0,
    'command': Commands.Agent_queues,
    'broker': {
        'host': '',
        'task': '',
        'result': ''
    },
    'result': False
}


Client_queues = {
    'command': Commands.Client_queues,
    'token': '',
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

Register_agent = {
    'id': 0,
    'command': Commands.Register_agent,
    'token': 'unified',
    'name': '',
    'broker': {
        'host': '',
        'task': '',
        'result': ''
    },
    'result': False
}
