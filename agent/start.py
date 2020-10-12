import logging

from time import sleep, time

from agent import Agent
from common.constants import AGENT, BROKER, SECOND
from common.logging_tools import setup_module_logger

PULSE_PERIOD = 10 * SECOND

modules = [__name__, AGENT, BROKER]
for module_name in modules:
    setup_module_logger(module_name, logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    with Agent() as agent:
        agent.connect()
        logger.info('Registering')
        agent.register()
        logger.info('Starting processing')
        while True:
            timestamp = time()
            for task in agent.broker.pulling_generator():
                pass
            delta = time() - timestamp
            logger.debug(f'Exit task loop after {delta:.3f} seconds')
            if delta < PULSE_PERIOD:
                delay = PULSE_PERIOD - delta
                logger.debug(f'Sleep for {delay} seconds')
                sleep(delay)
            agent.pulse()


if __name__ == '__main__':
    main()
