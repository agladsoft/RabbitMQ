import os
import json
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()

# os.environ['TOKEN_TELEGRAM'] = '6557326533:AAGcYU6q0DFLJfjZMHTaDjLnf-PzRMzA-6M'
# os.environ['XL_IDP_PATH_RABBITMQ'] = '/home/timur/sambashare/RabbitMQ'
# os.environ['XL_IDP_ROOT_RABBITMQ'] = '/home/timur/PycharmWork/RabbitMQ'


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


BATCH_SIZE: int = 5000
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"
LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
LOG_DIR_NAME: str = f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/logging"
LOG_FILE: str = f"{LOG_DIR_NAME}/processed_messages.json"
LOG_TABLE: str = "rmq_log"
with open(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/config/queues_config.json", "r", encoding="utf-8") as file:
    QUEUES_AND_ROUTING_KEYS: dict = json.load(file)
with open(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/config/tables_config.json", "r", encoding="utf-8") as file:
    TABLE_NAMES: dict = json.load(file)


def get_file_handler(name: str) -> logging.FileHandler:
    if not os.path.exists(LOG_DIR_NAME):
        os.mkdir(LOG_DIR_NAME)
    file_handler = RotatingFileHandler(
        filename=f"{LOG_DIR_NAME}/{name}.log",
        mode='a',
        maxBytes=20 * pow(1024, 2),
        backupCount=3
    )
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FTM))
    return file_handler


def get_stream_handler():
    stream_handler: logging.StreamHandler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return stream_handler


def get_logger(name: str) -> logging.getLogger:
    logger: logging.getLogger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(get_file_handler(name))
    logger.addHandler(get_stream_handler())
    logger.setLevel(logging.INFO)
    return logger


class MissingEnvironmentVariable(Exception):
    pass
