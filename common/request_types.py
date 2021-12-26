class Commands:
    Agent_queues = 'agent_queues'
    Client_queues = 'client_queues'
    Disconnect = 'disconnect'
    Pulse = 'pulse'
    Register_agent = 'register_agent'
    Relay = 'relay'


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

Disconnect = {
    'id': 0,
    'command': Commands.Disconnect,
    'result': False
}

Pulse = {
    'id': 0,
    'command': Commands.Pulse,
    'reply': {},
    'result': False
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
