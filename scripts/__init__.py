import os
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"
QUEUES_AND_ROUTING_KEYS: dict = {
    "DC_ACCOUNTING_DOCUMENTS_REQUESTS_QUEUE": "DC_ACCOUNTING_DOCUMENTS_REQUESTS_RT",
    "DC_ACCOUNTS_QUEUE": "DC_ACCOUNTS_RT",
    "DC_AUTOVISITS_QUEUE": "DC_AUTOVISITS_RT",
    "DC_AUTO_PICKUP_GENERAL_REPORT_QUEUE": "DC_AUTO_PICKUP_GENERAL_REPORT_RT",
    "DC_COMPLETED_REPACKAGES_REPORT_QUEUE": "DC_COMPLETED_REPACKAGES_REPORT_RT",
    "DC_CONSIGNMENTS_QUEUE": "DC_CONSIGNMENTS_RT",
    "DC_COUNTERPARTIES_QUEUE": "DC_COUNTERPARTIES_RT",
    "DC_DAILY_SUMMARY_QUEUE": "DC_DAILY_SUMMARY_RT",
    "DC_DATACORE_FREIGHT_QUEUE": "DC_DATACORE_FREIGHT_RT",
    "DC_DEVELOPMENT_COUNTERPARTY_BY_DEPARTMENT_QUEUE": "DC_DEVELOPMENT_COUNTERPARTY_BY_DEPARTMENT_RT",
    "DC_EXPORT_BOOKINGS_QUEUE": "DC_EXPORT_BOOKINGS_RT",
    "DC_FREIGHT_RATES_QUEUE": "DC_FREIGHT_RATES_RT",
    "DC_IMPORT_BOOKINGS_QUEUE": "DC_IMPORT_BOOKINGS_RT",
    "DC_MARGINALITY_ORDERS_BY_ACT_DATE_QUEUE": "DC_MARGINALITY_ORDERS_BY_ACT_DATE_RT",
    "DC_NATURAL_INDICATORS_BY_CONTRACTS_SEGMENTS_QUEUE": "DC_NATURAL_INDICATORS_BY_CONTRACTS_SEGMENTS_RT",
    "DC_NATURAL_INDICATORS_BY_TRANSACTION_FACT_DATE_QUEUE": "DC_NATURAL_INDICATORS_BY_TRANSACTION_FACT_DATE_RT",
    "DC_NATURAL_INDICATORS_OF_RAILWAY_RECEPTION_AND_DISPATCH_QUEUE":
        "DC_NATURAL_INDICATORS_OF_RAILWAY_RECEPTION_AND_DISPATCH_RT",
    "DC_ORDERS_MARGINALITY_REPORT_QUEUE": "DC_ORDERS_MARGINALITY_REPORT_RT",
    "DC_ORDERS_REPORT_QUEUE": "DC_ORDERS_REPORT_RT",
    "DC_REFERENCE_LOCATIONS_QUEUE": "DC_REFERENCE_LOCATIONS_RT",
    "DC_RUSCON_PRODUCTS_QUEUE": "DC_RUSCON_PRODUCTS_RT",
    "DC_RZHD_BY_OPERATIONS_REPORT_QUEUE": "DC_RZHD_BY_OPERATIONS_REPORT_QUEUE_RT",
    "DC_SALES_PLAN_QUEUE": "DC_SALES_PLAN_RT",
    "DC_TERMINALS_CAPACITY_QUEUE": "DC_TERMINALS_CAPACITY_RT",
    "DC_TRANSPORT_UNITS_QUEUE": "DC_TRANSPORT_UNITS_RT",
    "DO_MANAGER_EVALUATION_QUEUE": "DO_MANAGER_EVALUATION_RT",
    "DO_REFERENCE_COUNTERPARTIES_QUEUE": "DO_REFERENCE_COUNTERPARTIES_RT"
}

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
    "МаржинальностиСделокНаОсновеАктов":
        "marginality_orders_by_act_date",
    "ДокументКоммерческоеПредложение":
        "ruscon_products",
    "СправочникМестоположения":
        "reference_locations",
    "ФактическаяЕмкостьТерминала":
        "terminals_capacity",

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
LOG_FILE: str = f"{LOG_DIR_NAME}/processed_messages.json"


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
