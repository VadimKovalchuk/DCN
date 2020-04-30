from common.connection import RequestConnection


def main():
    connection = RequestConnection(port=9999)
    print("Connecting to hello world server…")

    #  Do 10 requests, waiting each time for a response
    for request in range(10):
        print("Sending request %s …" % request)
        message = connection.send(b'Hello')
        print("Received reply %s [ %s ]" % (request, message))


if __name__ == '__main__':
    main()
