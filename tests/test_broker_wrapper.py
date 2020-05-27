import logging

from common.broker import Broker

logger = logging.getLogger(__name__)

test_queue = 'test'


def test_broker_smoke():
    with Broker('localhost') as sender:
        sender.connect()
        sender.declare(output_queue=test_queue)
        with Broker('localhost') as receiver:
            receiver.connect()
            receiver.declare(input_queue=test_queue)
            receiver._inactivity_timeout = 0.1
            # Exchange
            logger.debug(sender.output_queue)
            logger.debug(receiver.input_queue)
            for _ in range(10):
                sender.push(str(_))
            receiver.pull()


