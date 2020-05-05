from copy import deepcopy
from time import sleep

from common.connection import RequestConnection
from common.request_types import register, pulse



def main():
    connection = RequestConnection(port=9999)
    print("Connecting to dispatcher")
    reply = connection.send(register)
    print(reply)
    if reply and reply['result']:
        agent_id = reply['id']
    else:
        assert False, 'Failed to get ID'
    _pulse = deepcopy(pulse)
    _pulse['id'] = agent_id
    #  Do 10 requests, waiting each time for a response
    for request in range(10):
        print("Sending pulse: %s" % request)
        message = connection.send(_pulse)
        print("Received pulse reply %s [ %s ]" % (request, message))
        sleep(1)


if __name__ == '__main__':
    main()
