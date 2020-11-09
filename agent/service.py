import logging
import sys

from time import sleep, monotonic

from agent import Agent, TaskRunner
from common.constants import AGENT, BROKER, SECOND
from common.logging_tools import setup_module_logger

PULSE_PERIOD = 10 * SECOND

modules = [__name__, AGENT, BROKER]
for module_name in modules:
    setup_module_logger(module_name, logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    dispatcher_host, token = sys.argv[1:]
    with Agent(dsp_host=dispatcher_host, token=token) as agent:
        agent.connect()
        logger.info('Registering')
        agent.register()
        logger.info('Starting processing')
        while True:
            timestamp = monotonic()
            for task in agent.broker.pulling_generator():
                runner = TaskRunner(task)
                runner.run()
                agent.broker.push(runner.report, runner.report['client'])
                agent.broker.set_task_done(task)
            delta = monotonic() - timestamp
            logger.debug(f'Exit task loop after {delta:.3f} seconds')
            if delta < PULSE_PERIOD:
                delay = PULSE_PERIOD - delta
                logger.debug(f'Sleep for {delay} seconds')
                sleep(delay)
            agent.pulse()


if __name__ == '__main__':
    main()
