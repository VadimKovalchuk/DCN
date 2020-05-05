from copy import deepcopy
from common.connection import RequestConnection
from common.request_types import register, pulse


def main():
    connection = RequestConnection(port=9999)
    print("Connecting to dispatcher")
    _register = deepcopy(register)
    _register['type'] = 'client'
    reply = connection.send(_register)
    print(reply)
    if reply and reply['result']:
        agent_id = reply['id']
    else:
        assert False, 'Failed to get ID'
    _pulse = deepcopy(pulse)
    _pulse['id'] = agent_id
    print("Sending pulse: %s" % 1)
    message = connection.send(_pulse)
    print("Received pulse reply %s [ %s ]" % (1, message))


if __name__ == '__main__':
    main()
