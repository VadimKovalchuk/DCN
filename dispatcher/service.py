import logging
import sys

from pathlib import Path

from dispatcher import Dispatcher
from common.constants import BROKER, DISPATCHER
from common.logging_tools import get_datetime_stamp, setup_module_logger

logger = logging.getLogger(__name__)

def main():
    broker_host = sys.argv[1]
    with Dispatcher(broker_host=broker_host) as dispatcher:
        dispatcher.connect()
        logger.info('Start listening')
        dispatcher.listen()


if __name__ == '__main__':
    log_folder = Path(f'log/dispatcher/{get_datetime_stamp()}_log.txt')
    log_folder.parent.mkdir(parents=True, exist_ok=True)
    modules = [__name__, BROKER, DISPATCHER]
    for module_name in modules:
        setup_module_logger(module_name, logging.DEBUG, log_folder)
    main()
