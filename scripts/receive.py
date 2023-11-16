import re
import sys
import time
import json
import contextlib
from __init__ import *
from pathlib import Path
from datetime import datetime
from rabbit_mq import RabbitMq
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from typing import Tuple, Union, Optional, Any


date_formats: tuple = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%d.%m.%Y %H:%M:%S")


class Receive(RabbitMq):
    def __init__(self):
        super().__init__()
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_")
                                                    + str(datetime.now().date()))

    def read_msg(self) -> None:
        """
        Connecting to a queue and receiving messages
        :return:
        """
        self.logger.info('The script has started working')
        self.read_text_msg(do_read_file=False)
        channel, connection = self.connect_rabbit()
        self.logger.info('Success connect to RabbitMQ')
        channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        channel.queue_declare(queue=self.queue_name, durable=self.durable)
        channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        self.logger.info("Start consuming")
        channel.start_consuming()
        self.logger.info('The script has completed working')

    def read_text_msg(self, do_read_file: bool = False) -> None:
        """

        :param do_read_file:
        :return:
        """
        if do_read_file:
            with open(f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/"
                      f"2023-11-15 10:00:54.617604-text_msg.json", 'r') as file:
                self.callback(ch='', method='', properties='', body=json.loads(file.read()))

    def callback(self, ch: str, method: str, properties: str, body) -> None:
        """
        Working with the message body
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_")
                                                    + str(datetime.now().date()))
        self.logger.info(f"Callback start for ch={ch}, method={method}, properties={properties}, body_message called")
        time.sleep(self.time_sleep)
        self.save_text_msg(body)
        data, file_name, dat_core = self.read_json(body)
        if dat_core:
            dat_core.count_number_loaded_rows(data, len(data), file_name)
        self.logger.info("Callback exit. The data from the queue was processed by the script")

    @staticmethod
    def save_text_msg(msg: Union[bytes, bytearray]) -> None:
        """

        :param msg:
        :return:
        """
        if isinstance(msg, (bytes, bytearray)):
            file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/{datetime.now()}-text_msg.json"
            fle: Path = Path(file_name)
            json_msg = json.loads(msg.decode('utf8'))
            if not os.path.exists(os.path.dirname(fle)):
                os.makedirs(os.path.dirname(fle))
            with open(file_name, 'w') as file:
                json.dump(json_msg, file, indent=4, ensure_ascii=False)

    @staticmethod
    def convert_format_date(date: str, data: dict, column) -> str:
        """operationMonth
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                date_file: datetime.date = datetime.strptime(date, date_format).date()
                date_clickhouse_access: datetime.date = datetime.strptime("1925-01-01", "%Y-%m-%d").date()
                if date_file < date_clickhouse_access:
                    data['OriginalDate'] += f"({column}: {date_file})\n"
                    return str(date_clickhouse_access)
                return str(date_file)
        return date

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        pass

    def parse_data(self, data, data_core: Any, eng_table_name: str) -> str:
        file_name: str = f"data_core_{datetime.now()}.json"
        len_rows: int = len(data)
        self.logger.info(f'Starting read json. Length of json is {len(data)}. Table is {eng_table_name}')
        for d in data:
            # if len(d) != 24:
            #     raise ValueError(f"The number of columns does not match in {d}")
            self.add_new_columns(len_rows, d, file_name)
            data_core.change_columns(d)
            d['OriginalDate'] = d['OriginalDate'].strip() if d['OriginalDate'] else None
        self.write_to_json(data, eng_table_name)
        return file_name

    def read_json(self, msg: str) -> Tuple[list, Optional[str], Any]:
        """
        Decoding a message and working with data.
        :param msg:
        :return:
        """
        msg: str = msg.decode('utf-8-sig') if isinstance(msg, (bytes, bytearray)) else msg
        all_data: dict = json.loads(msg) if isinstance(msg, str) else msg
        rus_table_name: str = all_data.get("header", {}).get("report")
        eng_table_name: str = TABLE_NAMES.get(rus_table_name)
        data: list = all_data.get("data", [])
        data_core: Any = CLASS_NAMES_AND_TABLES.get(eng_table_name)
        if data_core:
            data_core.table = eng_table_name
            data_core: Any = data_core()
            file_name: str = self.parse_data(data, data_core, eng_table_name)
            return data, file_name, data_core
        return data, None, data_core

    @staticmethod
    def add_new_columns(len_rows: int, data: dict, file_name: str) -> None:
        """
        Adding new columns.
        :param len_rows:
        :param data:
        :param file_name:
        :return:
        """
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = None
        data['is_obsolete_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data['len_rows'] = len_rows
        data['OriginalDate'] = ''

    @staticmethod
    def write_to_json(msg: str, eng_table_name, dir_name: str = "json") -> None:
        """
        Write data to json file
        :param msg:
        :param eng_table_name:
        :param dir_name:
        :return:
        """
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/{dir_name}/{datetime.now()}_{eng_table_name}.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg, file, indent=4, ensure_ascii=False)


class DataCoreClient(Receive):
    def __init__(self):
        super().__init__()
        self.client: Client = self.connect_to_db()

    @property
    def table(self):
        raise NotImplementedError(f'Define table name in {self.__class__.__name__}.')

    @table.setter
    def table(self, table: str):
        self.table: str = table

    @property
    def deal(self):
        raise NotImplementedError(f'Define deal in {self.__class__.__name__}.')

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
            sys.exit(1)
        return client

    def count_number_loaded_rows(self, data: list, count_rows: int, file_name: str) -> None:
        """
        Counting the number of rows to update transaction data.
        :return:
        """
        while count_rows != self.client.query(
                f"SELECT count(*) FROM {self.table} WHERE original_file_parsed_on='{file_name}'"
        ).result_rows[0][0]:
            self.logger.info("The data has not yet been uploaded to the database")
            time.sleep(60)
        self.logger.info("The data has been uploaded to the database")
        self.update_status(data, file_name)

    def update_status(self, data: list, file_name: str) -> None:
        """
        Updating the transaction by parameters.
        :return:
        """
        self.client.query(f"""
            ALTER TABLE {self.table}
            UPDATE is_obsolete=false
            WHERE original_file_parsed_on='{file_name}'
        """)
        self.logger.info("Success updated `is_obsolete` key on `False`")
        if self.deal:
            group_list: list = list({dictionary[self.deal]: dictionary for dictionary in data}.values())
            for item in group_list:
                self.client.query(f"""
                    ALTER TABLE {self.table} 
                    UPDATE is_obsolete=true, is_obsolete_date='{item['is_obsolete_date']}'
                    WHERE original_file_parsed_on != '{file_name}' AND is_obsolete=false 
                    AND {self.deal}='{item[self.deal]}'
                """)
            self.logger.info("Success updated all `is_obsolete` key")
        self.logger.info("Data processing in the database is completed")

    def delete_deal(self) -> None:
        """
        Deleting an is_obsolete key transaction.
        :return:
        """
        self.client.query(f"DELETE FROM {self.table} WHERE is_obsolete=true")
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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['voyageDate', 'operationDate']
        numeric_columns: list = ['containerCount', 'containerSize', 'operationMonth']
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        data['booking_list'] = data.get('bl')
        data['voyageMonth'] = datetime.strptime(
            self.convert_format_date(data.get('voyageMonth'), data, 'voyageMonth'), "%Y-%m-%d"
        ).month if data.get('voyageMonth') else None


class NaturalIndicatorsContractsSegments(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        year: Optional[str] = data.get('Year')
        month: Optional[str] = data.get('Month')
        date: Optional[str] = data.get('Date')
        data['Year'] = int(year) if year else None
        data['Month'] = int(month) if month else None
        data['Date'] = self.convert_format_date(date, data, 'Date') if date else None


class CounterParties(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "rc_uid"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        is_supplier: Optional[str] = data.get("is_supplier")
        is_foreign_company: Optional[str] = data.get("is_foreign_company")
        is_client: Optional[str] = data.get("is_client")
        is_other: Optional[str] = data.get("is_other")
        data["is_supplier"] = is_supplier == "Да" if is_supplier is not None else None
        data["is_foreign_company"] = is_foreign_company == "Да" if is_foreign_company is not None else None
        data["is_client"] = is_client == "Да" if is_client is not None else None
        data["is_other"] = is_other == "Да" if is_other is not None else None


class OrdersReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        data['booking_list'] = data.get('bl')


class AutoPickupGeneralReport(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = [
            'dateEmptyDelivery_fact', 'dateEmptyDelivery_plan', 'dateLoading_fact', 'dateDelivery_fact',
            'dateReceiptEmpty_fact', 'dateDelivery_plan', 'dateLoading_plan', 'dateReceiptEmpty_plan'
        ]
        numeric_columns: list = [
            'overpayment', 'totalRate', 'amountDowntime', 'rateAgreed',
            'containerSize', 'amountOverload', 'rateCarrier', 'economy', 'amountAddExpense'
        ]

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(re.sub(r'\s', '', str(data.get(column)))) if data.get(column) else None


class TransportUnits(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return None


class Consignments(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['voyageDate']
        numeric_columns: list = ['containerSize', 'teus', 'year']
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None
        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        data['booking_list'] = data.get('bl')


class SalesPlan(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return None

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        data['section'] = int(data.get('section')) if data.get('section') else None


class NaturalIndicatorsTransactionFactDate(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['operationDate', 'orderDate']
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
        return None


class ExportBookings(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['cargoReadiness', 'ETD', 'ETA', 'bookingDate']
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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['ETD', 'ETA', 'bookingDate']
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


if __name__ == '__main__':
    CLASSES: list = [
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
        ImportBookings
    ]
    CLASS_NAMES_AND_TABLES = {
        table_name: class_name
        for table_name, class_name in zip(list(TABLE_NAMES.values()), CLASSES)
    }
    Receive().read_msg()
