import logging
from pathlib import Path
from datetime import datetime
import os


def get_logger(
    logger_name: str,
    log_dir: str = './logs',
    level=logging.DEBUG
) -> logging.Logger:

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    log_dir = Path(log_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = log_dir.joinpath(datetime.now().strftime(("%d%m%Y_%H%M")))

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
