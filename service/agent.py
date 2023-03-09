import logging
import sys

from pathlib import Path
from time import sleep, monotonic

from dcn.agent import Agent, TaskRunner
from dcn.common.constants import AGENT, BROKER, SECOND
from dcn.common.logging_tools import get_datetime_stamp, setup_module_logger

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
                agent.request_broker_data()
            if agent.broker and not agent.broker.is_connected:
                agent.broker.connect()
            if agent.broker and agent.broker.is_connected:
                status, task = agent.broker.consume()
                if status and task:
                    runner = TaskRunner(task)
                    runner.run()
                    agent.broker.publish(runner.report, runner.report['client'])
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
        (AGENT, logging.DEBUG),
        (BROKER, logging.INFO)
    ]
    for module_name, level in log_config:
        setup_module_logger(module_name, level, log_folder)
    main()
