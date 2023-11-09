import os
import logging
from dotenv import load_dotenv


load_dotenv()

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"

LIST_TABLES = [
    "counterparties",
    "datacore_freight",
    "datacore_segment"
]

TABLE_NAMES: dict = {
    "СписокКонтрагентов": LIST_TABLES[0],
    "ОтчетПоКонтролируемомуИНеконтролируемомуФрахту": LIST_TABLES[1],
    "ОтчетНатуральныеПоказателиПоСделкамИСегментам": LIST_TABLES[2]
}

# os.environ['XL_IDP_PATH_RABBITMQ'] = '/home/timur/sambashare/RabbitMQ'
# os.environ['XL_IDP_ROOT_RABBITMQ'] = '/home/timur/PycharmWork/docker_project/RabbitMQ'


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


def get_file_handler(name: str) -> logging.FileHandler:
    log_dir_name: str = f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler: logging.FileHandler = logging.FileHandler(f"{log_dir_name}/{name}.log")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FTM))
    return file_handler


def get_logger(name: str) -> logging.getLogger:
    logger: logging.getLogger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(get_file_handler(name))
    logger.setLevel(logging.INFO)
    return logger


class MissingEnvironmentVariable(Exception):
    pass
