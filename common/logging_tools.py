import logging

from pathlib import Path

default_log_folder = Path('/tmp/finNet/dispatcher.txt')
default_log_folder.parent.mkdir(parents=True, exist_ok=True)
formatter = logging.Formatter('%(asctime)s-%(name)s:%(lineno)d-'
                              '%(levelname)s-%(message)s')


def setup_module_logger(module_name: str,
                        level: int,
                        file_pah: Path = default_log_folder):
    logger = logging.getLogger(module_name)
    logger.setLevel(level)

    file_handler = logging.FileHandler(file_pah)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info(f'Logger for {module_name} is started')
