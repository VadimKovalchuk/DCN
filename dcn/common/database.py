

FAKE_DB = {
    'agents': {
        'localhost': {
            'broker': 'localhost'
        },
        'docker': {
            'broker': 'rabbitmq'
        }
    },
    'clients': {
        'localhost': {
            'broker': 'localhost'
        },
        'docker': {
            'broker': 'rabbitmq'
        }
    }
}


class Database:
    def __init__(self):
        pass

    def get_client_param(token: str):
        return FAKE_DB['clients'].get(token)

    def get_agent_param(token: str):
        return FAKE_DB['agents'].get(token)
