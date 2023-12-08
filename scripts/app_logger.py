import os
import logging

_log_format: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
_dateftm: str = "%d/%B/%Y %H:%M:%S"


def get_file_handler(name: str) -> logging.FileHandler:
    log_dir_name: str = f"{os.environ.get('XL_IDP_ROOT_RABBITMQ')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler: logging.FileHandler = logging.FileHandler(f"{log_dir_name}/{name}.log")
    file_handler.setFormatter(logging.Formatter(_log_format, datefmt=_dateftm))
    return file_handler


def get_logger(name: str) -> logging.getLogger:
    logger: logging.getLogger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(get_file_handler(name))
    logger.setLevel(logging.INFO)
    return logger