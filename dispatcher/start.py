import logging

from dispatcher import Dispatcher
from common.constants import BROKER, DISPATCHER
from common.logging_tools import setup_module_logger


modules = [__name__,BROKER, DISPATCHER]
for module_name in modules:
    setup_module_logger(module_name, logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    with Dispatcher() as dispatcher:
        dispatcher.connect()
        dispatcher.listen()


if __name__ == '__main__':
    main()
