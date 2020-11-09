import logging
import sys

from dispatcher import Dispatcher
from common.constants import BROKER, DISPATCHER
from common.logging_tools import setup_module_logger


modules = [__name__, BROKER, DISPATCHER]
for module_name in modules:
    setup_module_logger(module_name, logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    broker_host = sys.argv[1]
    with Dispatcher(broker_host=broker_host) as dispatcher:
        dispatcher.connect()
        logger.info('Start listening')
        dispatcher.listen()


if __name__ == '__main__':
    main()
