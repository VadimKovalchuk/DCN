import logging
import sys

from pathlib import Path
from time import sleep, monotonic

from agent import Agent, TaskRunner
from common.constants import AGENT, BROKER, SECOND
from common.logging_tools import get_datetime_stamp, setup_module_logger

PULSE_PERIOD = 10 * SECOND

logger = logging.getLogger(__name__)


def main():
    dispatcher_host, token = sys.argv[1:]
    with Agent(dsp_host=dispatcher_host, token=token) as agent:
        logger.info('Registering')
        registered = False
        logger.info('Starting processing')
        while True:
            timestamp = monotonic()

            if not registered:
                registered = agent.register()
            if registered and not agent.broker:
                all((
                    agent.request_broker_data(),
                    agent.broker.ensure_connection(),
                    agent.broker.declare()
                ))
            if agent.broker and agent.broker.connected:
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
            if registered:
                agent.pulse()


if __name__ == '__main__':
    log_folder = Path(f'log/agent/{get_datetime_stamp()}_log.txt')
    log_folder.parent.mkdir(parents=True, exist_ok=True)
    log_config = [
        (__name__, logging.DEBUG),
        (AGENT, logging.INFO),
        (BROKER, logging.INFO)
    ]
    for module_name, level in log_config:
        setup_module_logger(module_name, level, log_folder)
    main()
