import re
import sys
import pytz
import json
import copy
import requests
import contextlib
from __init__ import *
from pathlib import Path
from pika.spec import Basic
from rabbit_mq import RabbitMq
from pika import BasicProperties
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from datetime import datetime, date, timedelta
from typing import Tuple, Union, Optional, Any
from pika.adapters.blocking_connection import BlockingChannel

date_formats: tuple = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%d.%m.%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%d.%m.%Y",
    "%Y-%m-%d"
)
tz: pytz.timezone = pytz.timezone("Europe/Moscow")
message_errors: list = []
upload_tables: set = set()


def serialize_datetime(obj):
    if isinstance(obj, datetime) or isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Type not serializable")


class Receive(RabbitMq):
    def __init__(self):
        super().__init__()
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_")
                                                    + str(datetime.now(tz=tz).date()))
        self.count_message: int = 0

    def read_msg(self) -> None:
        """
        Connecting to a queue and receiving messages
        :return:
        """
        self.logger.info('The script has started working')
        self.read_text_msg(do_read_file=eval(get_my_env_var('DO_READ_FILE')))
        self.connect_rabbit()
        self.logger.info('Success connect to RabbitMQ')
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        self.channel.queue_declare(queue=self.queue_name, durable=self.durable)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=False)
        self.logger.info("Start consuming")
        self.channel.start_consuming()
        self.logger.info('The script has completed working')

    def check_queue_empty(self):
        """
        Checking the number of messages in the queue
        :return:
        """
        global message_errors, upload_tables
        method_frame, header_frame, body = self.channel.basic_get(self.queue_name)
        if method_frame is None:
            message = f"Очередь '{self.queue_name}' пустая. Загруженные таблицы - {upload_tables}. " \
                      f"Количество ошибок - {len(message_errors)}. Ошибки - {message_errors}"
            self.logger.info(message)
            url = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
            params = {
                "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
                "text": message,
                "reply_to_message_id": get_my_env_var('MESSAGE_ID')
            }
            message_errors = []
            upload_tables = set()
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        else:
            self.channel.basic_nack(method_frame.delivery_tag)

    def read_text_msg(self, do_read_file: bool = False) -> None:
        """

        :param do_read_file:
        :return:
        """
        if do_read_file:
            with open(f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/"
                      f"{get_my_env_var('FILE_NAME')}", 'r') as file:
                self.callback(
                    ch='',
                    method='',
                    properties='',
                    body=json.loads(file.read().encode().decode('utf-8-sig'))
                )

    def callback(
            self,
            ch: Union[BlockingChannel, str],
            method: Union[Basic.Deliver, str],
            properties: Union[BasicProperties, str],
            body: Union[bytes, str]
    ) -> None:
        """
        Working with the message body
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        try:
            self.count_message += 1
            self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_")
                                                        + str(datetime.now(tz=tz).date()))
            self.logger.info(f"Callback start for ch={ch}, method={method}, properties={properties}, "
                             f"body_message called. Count messages is {self.count_message}")
            all_data, data, file_name, data_core, key_deals = self.read_json(body)
            if data_core:
                data_core.handle_rows(all_data, data, file_name, key_deals)
            else:
                message_errors.append(key_deals)
                data_core_client = DataCoreClient
                data_core_client.table = all_data.get("header", {}).get("report")
                data_core_client().insert_message(all_data, key_deals, is_success_inserted=False)
            self.logger.info("Callback exit. The data from the queue was processed by the script")
        except AssertionError:
            pass
        except ConnectionError as ex:
            self.logger.error(f"ConnectionError is {ex}")
            msg: str = body.decode('utf-8-sig') if isinstance(body, (bytes, bytearray)) else body
            all_data: dict = json.loads(msg) if isinstance(msg, str) else msg
            self.write_to_json(all_data, "unknown", dir_name="errors")
        finally:
            self.check_queue_empty()
            delivery_tag = method.delivery_tag if not isinstance(method, str) else None
            self.channel.basic_ack(delivery_tag=delivery_tag) if delivery_tag else None

    @staticmethod
    def save_text_msg(msg: Union[bytes, bytearray]) -> None:
        """

        :param msg:
        :return:
        """
        if isinstance(msg, (bytes, bytearray)):
            json_msg = json.loads(msg.decode('utf8'))
            file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/" \
                             f"{datetime.now(tz=tz)}-{json_msg['header']['report']}-text_msg.json"
            fle: Path = Path(file_name)
            if not os.path.exists(os.path.dirname(fle)):
                os.makedirs(os.path.dirname(fle))
            with open(file_name, 'w') as file:
                json.dump(json_msg, file, indent=4, ensure_ascii=False, default=serialize_datetime)

    def parse_data(self, all_data: dict, data, data_core: Any, eng_table_name: str, key_deals: str) -> str:
        """

        :param all_data:
        :param data:
        :param data_core:
        :param eng_table_name:
        :param key_deals:
        :return:
        """
        file_name: str = f"{eng_table_name}_{datetime.now(tz=tz)}.json"
        self.logger.info(f'Starting read json. Length of json is {len(data)}. Table is {eng_table_name}')
        list_columns_db: list = data_core.get_table_columns()
        original_date_string: str = data_core.original_date_string
        [list_columns_db.remove(remove_column) for remove_column in data_core.removed_columns_db]
        try:
            for i, row in enumerate(data):
                data[i] = data_core.convert_to_lowercase(row)
                data_core.add_new_columns(data[i], file_name, original_date_string)
                data_core.change_columns(data[i])
                if original_date_string:
                    data[i][original_date_string] = data[i][original_date_string].strip() \
                        if data[i][original_date_string] else None
        except Exception as ex:
            self.logger.error(f"An error was received when converting data types. "
                              f"Table is {eng_table_name}. Exception is {ex}")
            data_core.message_errors.append(key_deals)
            self.write_to_json(all_data, eng_table_name, dir_name="errors")
            data_core.insert_message(all_data, key_deals, is_success_inserted=False)
            raise AssertionError("Stop consuming because receive an error where converting data types")
        if data:
            list_columns_rabbit: list = list(data[0].keys())
            data_core.check_difference_columns(all_data, eng_table_name, list_columns_db, list_columns_rabbit,
                                               key_deals)
        return file_name

    def read_json(self, msg: str) -> Tuple[dict, list, Optional[str], Any, str]:
        """
        Decoding a message and working with data.
        :param msg:
        :return:
        """
        msg: str = msg.decode('utf-8-sig') if isinstance(msg, (bytes, bytearray)) else msg
        all_data: dict = json.loads(msg) if isinstance(msg, str) else msg
        rus_table_name: str = all_data.get("header", {}).get("report")
        key_deals: str = all_data.get("header", {}).get("key_id")
        eng_table_name: str = TABLE_NAMES.get(rus_table_name)
        upload_tables.add(eng_table_name)
        data: list = copy.deepcopy(all_data).get("data", [])
        data_core: Any = CLASS_NAMES_AND_TABLES.get(eng_table_name)
        if data_core:
            data_core.table = eng_table_name
            data_core: Any = data_core()
            file_name: str = self.parse_data(all_data, data, data_core, eng_table_name, key_deals)
            return all_data, data, file_name, data_core, key_deals
        self.logger.info(f"Not found table name in dictionary. Russian table is {rus_table_name}")
        upload_tables.add(rus_table_name)
        return all_data, data, None, data_core, key_deals

    def write_to_json(self, msg: dict, eng_table_name: str, dir_name: str = "json") -> None:
        """
        Write data to json file
        :param msg:
        :param eng_table_name:
        :param dir_name:
        :return:
        """
        self.logger.info(f"Saving data to file {datetime.now(tz=tz)}_{eng_table_name}.json")
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/{dir_name}/{datetime.now(tz=tz)}_{eng_table_name}" \
                         f".json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg, file, indent=4, ensure_ascii=False, default=serialize_datetime)


class DataCoreClient(Receive):
    def __init__(self):
        super().__init__()
        self.client: Client = self.connect_to_db()
        self.removed_columns_db = ['uuid']

    @property
    def database(self):
        return self.client.database

    @property
    def table(self):
        raise NotImplementedError(f'Define table name in {self.__class__.__name__}.')

    @table.setter
    def table(self, table: str):
        self.table: str = table

    @property
    def deal(self):
        raise NotImplementedError(f'Define deal in {self.__class__.__name__}.')

    @property
    def original_date_string(self):
        return None

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        pass

    def convert_format_date(self, date_: str, data: dict, column, is_datetime: bool = False) -> Union[datetime, str]:
        """
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                if not is_datetime:
                    date_file: Union[datetime.date, datetime] = datetime.strptime(date_, date_format).date()
                    date_db_access: Union[datetime.date, datetime] = datetime.strptime("1925-01-01", "%Y-%m-%d").date()
                else:
                    date_file = datetime.strptime(date_, date_format)
                    date_db_access = datetime.strptime("1925-01-01", "%Y-%m-%d")
                if date_file < date_db_access:
                    data[self.original_date_string] += f"({column}: {date_file})\n"
                    return date_db_access
                return date_file
        return date_

    @staticmethod
    def add_new_columns(data: dict, file_name: str, original_date_string: str) -> None:
        """
        Adding new columns.
        :param data:
        :param file_name:
        :param original_date_string:
        :return:
        """
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = False
        data['is_obsolete_date'] = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")
        if original_date_string:
            data[original_date_string] = ''

    @staticmethod
    def convert_to_lowercase(data: dict):
        """
        Convert keys of columns to lowercase text.
        :param data:
        :return:
        """
        return {k.lower(): v for k, v in data.items()}

    def check_difference_columns(
            self,
            all_data: dict,
            eng_table_name: str,
            list_columns_db: list,
            list_columns_rabbit: list,
            key_deals: str
    ) -> None:
        """

        :param all_data:
        :param eng_table_name:
        :param list_columns_db:
        :param list_columns_rabbit:
        :param key_deals:
        :return:
        """
        diff_db: list = list(set(list_columns_db) - set(list_columns_rabbit))
        diff_rabbit: list = list(set(list_columns_rabbit) - set(list_columns_db))
        if diff_db or diff_rabbit:
            self.logger.error(f"The difference in columns {diff_db} from the database. "
                              f"The difference in columns {diff_rabbit} from the rabbit")
            message_errors.append(key_deals)
            self.write_to_json(all_data, eng_table_name, dir_name="errors")
            self.insert_message(all_data, key_deals, is_success_inserted=False)
            raise AssertionError("Stop consuming because columns is different")

    def connect_to_db(self) -> Client:
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            client.query("SET allow_experimental_lightweight_delete=1")
            self.logger.info("Success connect to clickhouse")
        except Exception as ex_connect:
            self.logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            raise ConnectionError
        return client

    def get_table_columns(self):
        """

        :return:
        """
        described_table = self.client.query(f"DESCRIBE TABLE {self.database}.{self.table}")
        return described_table.result_columns[0]

    def insert_message(self, all_data: dict, key_deals: str, is_success_inserted: bool):
        """

        :param all_data:
        :param key_deals:
        :param is_success_inserted:
        :return:
        """
        len_data: int = 100
        all_data["data"] = all_data["data"][:len_data] if len(all_data["data"]) >= len_data else all_data["data"]
        rows = [[
            self.database,
            self.table,
            self.queue_name,
            key_deals,
            datetime.now(tz=tz) + timedelta(hours=3),
            is_success_inserted,
            json.dumps(all_data, default=serialize_datetime, ensure_ascii=False, indent=2)
        ]]
        columns = ["database", "table", "queue", "key_id", "datetime", "is_success", "message"]
        self.client.insert(table="rmq_log", database="DataCore", data=rows, column_names=columns)

    def handle_rows(self, all_data, data: list, file_name: str, key_deals: str) -> None:
        """
        Counting the number of rows to update transaction data.
        :param all_data:
        :param data:
        :param file_name:
        :param key_deals:
        :return:
        """
        try:
            rows = [list(row.values()) for row in data] if data else [[]]
            columns = [row for row in data[0]] if data else []
            if rows and columns:
                self.client.insert(table=self.table, database=self.database, data=rows, column_names=columns)
                self.logger.info("The data has been uploaded to the database")
            self.update_status(data, file_name, key_deals)
            self.insert_message(all_data, key_deals, is_success_inserted=True)
        except Exception as ex:
            self.logger.error(f"Exception is {ex}")
            message_errors.append(key_deals)
            self.write_to_json(all_data, self.table, dir_name="errors")
            self.insert_message(all_data, key_deals, is_success_inserted=False)

    def update_status(self, data: list, file_name: str, key_deals: str) -> None:
        """
        Updating the transaction by parameters.
        :return:
        """
        for item in data:
            query: str = f"ALTER TABLE {self.database}.{self.table} " \
                         f"UPDATE is_obsolete=true, is_obsolete_date='{item['is_obsolete_date']}' " \
                         f"WHERE original_file_parsed_on != '{file_name}' AND is_obsolete=false " \
                         f"AND {self.deal}='{item[self.deal]}'"
            self.client.query(query)
        if not data:
            query: str = f"ALTER TABLE {self.database}.{self.table} " \
                         f"UPDATE is_obsolete=true, is_obsolete_date='{datetime.now(tz=tz)}' " \
                         f"WHERE original_file_parsed_on != '{file_name}' AND is_obsolete=false " \
                         f"AND {self.deal}='{key_deals}'"
            self.client.query(query)
        self.logger.info("Data processing in the database is completed")

    def delete_old_deals(self, cond: str = "is_obsolete=true") -> None:
        """
        Deleting an is_obsolete key transaction.
        :return:
        """
        self.client.query(f"DELETE FROM {self.database}.{self.table} WHERE {cond}")
        self.logger.info("Successfully deleted old transaction data")

    def __exit__(self, exception_type, exception_val, trace):
        try:
            self.client.close()
            self.logger.info("Success disconnect clickhouse")
        except AttributeError:  # isn't closable
            self.logger.info("Not closable")
            return True


class DataCoreFreight(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_voyage_month_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['voyage_date', 'operation_date']
        numeric_columns: list = ['container_count', 'container_size', 'operation_month']

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None

        data['voyage_month'] = self.convert_format_date(data.get('voyage_month'), data, 'voyage_month').month \
            if data.get('voyage_month') else None


class NaturalIndicatorsContractsSegments(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['date']
        numeric_columns: list = ['year', 'month']

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None


class CounterParties(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"


class OrdersReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_voyage_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['voyage_date']

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class AutoPickupGeneralReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_date_delivery_plan_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = [
            'date_delivery_empty_fact', 'date_delivery_empty_plan', 'date_loading_fact',
            'date_delivery_fact', 'date_receiving_empty_fact', 'date_delivery_plan',
            'date_loading_plan', 'date_receiving_empty_plan'
        ]
        numeric_columns: list = ['container_size']
        float_columns: list = [
            'overpayment', 'downtime_amount', 'agreed_rate',
            'total_rate', 'carrier_rate', 'economy',
            'overload_amount', 'add_expense_amount'
        ]

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(re.sub(r'\s', '', str(data.get(column)))) if data.get(column) else None
        for column in float_columns:
            data[column] = float(re.sub(r'\s', '', str(data.get(column)))) if data.get(column) else None


class TransportUnits(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"


class Consignments(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_voyage_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['voyage_date']
        numeric_columns: list = ['container_size', 'teu', 'year']

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None


class SalesPlan(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['teu', 'container_count', 'container_size', 'year', 'month']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None


class NaturalIndicatorsTransactionFactDate(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_operation_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = [
            'container_size', 'operation_month', 'container_count',
            'teu', 'operation_year'
        ]
        date_columns: list = ['operation_date', 'order_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class DevelopmentCounterpartyDepartment(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['year']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None


class ExportBookings(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_booking_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['container_size', 'container_count', 'freight_rate']
        date_columns: list = ['cargo_readiness', 'etd', 'eta', 'booking_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class ImportBookings(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_booking_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['container_size', 'container_count', 'freight_rate']
        date_columns: list = ['etd', 'eta', 'booking_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class CompletedRepackagesReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_repacking_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = [
            'warehouse_wms_count', 'inspection_container_count', 'import_teu',
            'import_container_count', 'export_teu', 'export_container_count'
        ]
        date_columns: list = ['repacking_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class AutoVisits(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_entry_datetime_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['processing_time', 'waiting_time']
        date_columns: list = ['exit_datetime', 'entry_datetime', 'registration_datetime']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(
                data.get(column), data, column, is_datetime=True
            ) if data.get(column) else None


class AccountingDocumentsRequests(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_request_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['start_date', 'end_date', 'request_date']

        for column in date_columns:
            data[column] = self.convert_format_date(
                data.get(column), data, column, is_datetime=True
            ) if data.get(column) else None


class DailySummary(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_motion_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['cargo_weight', 'tare_weight', 'tonnage', 'container_size']
        date_columns: list = ['motion_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class RZHDOperationsReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_operation_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['container_size', 'operation_month', 'operation_year']
        date_columns: list = ['operation_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class OrdersMarginalityReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_order_creation_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        float_columns: list = [
            'expenses_rental_without_vat_fact', 'income_without_vat_fact', 'profit_plan',
            'income_without_vat_plan', 'expenses_without_vat_plan', 'expenses_without_vat_fact',
            'profit_fact'
        ]
        date_columns: list = ['order_creation_date']

        for column in float_columns:
            data[column] = float(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class NaturalIndicatorsRailwayReceptionDispatch(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['container_size', 'container_count', 'teu', 'internal_customs_transit']
        date_columns: list = ['date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class Accounts(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        float_columns: list = ['profit_account_rub', 'profit_account']
        date_columns: list = ['date']

        for column in float_columns:
            data[column] = float(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class FreightRates(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        float_columns: list = ['rate']
        numeric_columns: list = ['oversized_width', 'oversized_height', 'oversized_length']
        date_columns: list = ['expiration_date', 'start_date']
        bool_columns: list = ['priority', 'oversized', 'dangerous', 'special_rate']

        for column in float_columns:
            data[column] = float(re.sub(r'(?<=\d)\s+(?=\d)', '', str(data.get(column))).replace(",", ".")) \
                if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(re.sub(r'(?<=\d)\s+(?=\d)', '', str(data.get(column)))) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in bool_columns:
            if isinstance(data.get(column), str):
                data[column] = True if data.get(column).upper() == 'ДА' else False


class ManagerEvaluation(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def database(self):
        return "DO"

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['evaluation']
        date_columns: list = ['evaluation_date']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class ReferenceCounterparties(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def database(self):
        return "DO"

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "key_id"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        bool_columns: list = ['is_control', 'is_foreign_company']

        for column in bool_columns:
            if isinstance(data.get(column), str):
                data[column] = True if data.get(column).upper() == 'ДА' else False


if __name__ == '__main__':
    CLASSES: list = [
        # Данные по DC
        CounterParties,
        DataCoreFreight,
        NaturalIndicatorsContractsSegments,
        OrdersReport,
        AutoPickupGeneralReport,
        TransportUnits,
        Consignments,
        SalesPlan,
        NaturalIndicatorsTransactionFactDate,
        DevelopmentCounterpartyDepartment,
        ExportBookings,
        ImportBookings,
        CompletedRepackagesReport,
        AutoVisits,
        AccountingDocumentsRequests,
        DailySummary,
        RZHDOperationsReport,
        OrdersMarginalityReport,
        NaturalIndicatorsRailwayReceptionDispatch,
        Accounts,
        FreightRates,

        # Данные по оценкам менеджеров
        ManagerEvaluation,

        # Данные по справочнику контрагентов
        ReferenceCounterparties
    ]
    CLASS_NAMES_AND_TABLES: dict = {
        table_name: class_name
        for table_name, class_name in zip(list(TABLE_NAMES.values()), CLASSES)
    }
    Receive().read_msg()
