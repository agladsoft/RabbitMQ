import os
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"
HOUR: int = 19

TABLE_NAMES: dict = {
    # Данные по DC
    "СписокКонтрагентов":
        "counterparties",
    "ОтчетПоКонтролируемомуИНеконтролируемомуФрахту":
        "datacore_freight",
    "ОтчетНатуральныеПоказателиПоСделкамИСегментам":
        "natural_indicators_by_contracts_segments",
    "ОтчетПоПоручениям":
        "orders_report",
    "ОбщийОтчетПоАвтовывозу":
        "auto_pickup_general_report",
    "ВладельцыКонтейнеров":
        "transport_units",
    "СписокКоносаментов":
        "consignments",
    "РегистрСведенийПланПродаж":
        "sales_plan",
    "ОтчетПоНатуральнымНаОсновеОперацийПоФактическимДатам":
        "natural_indicators_by_transaction_fact_date",
    "РегистрСведенийКонтрагентыРазвитияПоЦФО":
        "development_counterparty_by_department",
    "ОтчетExportBookings":
        "export_bookings",
    "ОтчетImportBookings":
        "import_bookings",
    "ОтчетПоЗавершеннымПеретаркам":
        "completed_repackages_report",
    "ОтчетАвтовизиты":
        "autovisits",
    "ОтчетПоОбращениямВПЭО":
        "accounting_documents_requests",
    "ОтчетЕжедневнаяСводка":
        "daily_summary",
    "ОтчетПоЖДПеревозкамМаркетингПоОперациям":
        "rzhd_by_operations_report",
    "ОтчетПоМаржинальностиСделок":
        "orders_marginality_report",
    "ОтчетНатуральныеПоказателиПриемаИОтправкиПоЖД_TEU":
        "natural_indicators_of_railway_reception_and_dispatch",
    "СуммыСчетовПокупателям":
        "accounts",
    "СтавкиФрахта":
        "freight_rates",

    # Данные по оценкам менеджеров
    "ОценкиМенеджеров":
        "manager_evaluation",

    # Данные по справочнику контрагентов
    "СправочникКонтрагентовДО":
        "reference_counterparties",
}

LOG_TABLE: str = "rmq_log"

# os.environ['TOKEN_TELEGRAM'] = '6557326533:AAGcYU6q0DFLJfjZMHTaDjLnf-PzRMzA-6M'
# os.environ['XL_IDP_PATH_RABBITMQ'] = '/home/timur/sambashare/RabbitMQ'
# os.environ['XL_IDP_ROOT_RABBITMQ'] = '/home/timur/PycharmWork/RabbitMQ'


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


LOG_DIR_NAME: str = f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/logging"
LOG_FILE: str = f"{LOG_DIR_NAME}/processed_messages.log"


def get_file_handler(name: str) -> logging.FileHandler:
    if not os.path.exists(LOG_DIR_NAME):
        os.mkdir(LOG_DIR_NAME)
    file_handler = RotatingFileHandler(filename=f"{LOG_DIR_NAME}/{name}.log", mode='a', maxBytes=20.5 * pow(1024, 2),
                                       backupCount=100)
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
