import logging
import datetime

from pathlib import Path

default_log_folder = Path('/tmp/dcn/log.txt')
default_log_folder.parent.mkdir(parents=True, exist_ok=True)
formatter = logging.Formatter('%(asctime)s-%(name)s:%(lineno)d-'
                              '%(levelname)s-%(message)s')


def setup_module_logger(module_name: str,
                        level: int,
                        file_pah: Path = default_log_folder):
    """
    Starts logging collection for specified module.

    :param module_name: Logged module name
    :param level: module logging detailization
    :param file_pah: location for module logging
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(level)

    file_handler = logging.FileHandler(file_pah)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info(f'Logger for {module_name} is started')


def get_datetime_stamp(for_filename=True):
    stamp = datetime.datetime.now().isoformat()
    if for_filename:
        for symbol in ('-', ':', '.'):
            stamp.replace(symbol, '_')
    return stamp
