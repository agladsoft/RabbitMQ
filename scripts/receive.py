import re
import sys
import time
import json
import contextlib
from __init__ import *
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from rabbit_mq import RabbitMq
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from typing import Tuple, Union, Optional, Any


date_formats: tuple = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y"
)


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
        self.read_text_msg(do_read_file=eval(get_my_env_var('DO_READ_FILE')))
        self.connect_rabbit()
        self.logger.info('Success connect to RabbitMQ')
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        self.channel.queue_declare(queue=self.queue_name, durable=self.durable)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        self.logger.info("Start consuming")
        self.channel.start_consuming()
        self.logger.info('The script has completed working')

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
        data, file_name, data_core = self.read_json(body)
        if data_core:
            data_core.count_number_loaded_rows(data, len(data), file_name)
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
    def convert_format_date(date: str, data: dict, column, is_datetime: bool = False) -> str:
        """operationMonth
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                if not is_datetime:
                    date_file: Union[datetime.date, datetime] = datetime.strptime(date, date_format).date()
                    date_db_access: Union[datetime.date, datetime] = datetime.strptime("1925-01-01", "%Y-%m-%d").date()
                else:
                    date_file = datetime.strptime(date, date_format)
                    date_db_access = datetime.strptime("1925-01-01", "%Y-%m-%d")
                if date_file < date_db_access:
                    data['originalDateString'] += f"({column}: {date_file})\n"
                    return str(date_db_access)
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
        """

        :param data:
        :param data_core:
        :param eng_table_name:
        :return:
        """
        file_name: str = f"data_core_{datetime.now()}.json"
        self.logger.info(f'Starting read json. Length of json is {len(data)}. Table is {eng_table_name}')
        list_columns_db: list = data_core.get_table_columns()
        [list_columns_db.remove(remove_column) for remove_column in data_core.removed_columns_db]
        for d in data:
            self.add_new_columns(d, file_name)
            data_core.change_columns(d)
            d['originalDateString'] = d['originalDateString'].strip() if d['originalDateString'] else None
        list_columns_rabbit: list = list(data[0].keys())
        [list_columns_rabbit.remove(remove_column) for remove_column in data_core.removed_columns_rabbit]
        self.check_difference_columns(list_columns_db, list_columns_rabbit)
        self.write_to_json(data, eng_table_name)
        return file_name

    def check_difference_columns(self, list_columns_db: list, list_columns_rabbit: list) -> None:
        """

        :param list_columns_db:
        :param list_columns_rabbit:
        :return:
        """
        diff_db: list = list(set(list_columns_db) - set(list_columns_rabbit))
        diff_rabbit: list = list(set(list_columns_rabbit) - set(list_columns_db))
        if diff_db or diff_rabbit:
            self.logger.error(f"The difference in columns {diff_db} from the database. "
                              f"The difference in columns {diff_rabbit} from the rabbit")
            self.channel.basic_cancel()

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
    def add_new_columns(data: dict, file_name: str) -> None:
        """
        Adding new columns.
        :param data:
        :param file_name:
        :return:
        """
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = None
        data['is_obsolete_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data['originalDateString'] = ''

    def write_to_json(self, msg: str, eng_table_name, dir_name: str = "json") -> None:
        """
        Write data to json file
        :param msg:
        :param eng_table_name:
        :param dir_name:
        :return:
        """
        self.logger.info(f"Saving data to file {datetime.now()}_{eng_table_name}.json")
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
        self.removed_columns_db = ['uuid']
        self.removed_columns_rabbit = []

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

    def get_table_columns(self):
        """

        :return:
        """
        described_table = self.client.query(f"DESCRIBE TABLE {self.table}")
        return described_table.result_columns[0]

    def count_number_loaded_rows(self, data: list, count_rows: int, file_name: str) -> None:
        """
        Counting the number of rows to update transaction data.
        :return:
        """
        datetime_start: datetime = datetime.now()
        while count_rows != self.client.query(
                f"SELECT count(*) FROM {self.table} WHERE original_file_parsed_on='{file_name}'"
        ).result_rows[0][0] and datetime.now() - datetime_start < timedelta(minutes=60):
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
        self.removed_columns_rabbit = ['bl']

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
            self.convert_format_date(data.get('voyageMonth'), data, 'voyageMonth'),
            "%Y-%m-%d"
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
        date_columns: list = ['Date']
        numeric_columns: list = ['Year', 'Month']

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
        return "rc_uid"


class OrdersReport(DataCoreClient):
    def __init__(self):
        super().__init__()
        self.removed_columns_rabbit = ['bl']

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
        date_columns: list = ['voyageDateout']

        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None

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
            'dateEmptyDelivery_fact', 'dateEmptyDelivery_plan', 'dateLoading_fact',
            'dateDelivery_fact', 'dateReceiptEmpty_fact', 'dateDelivery_plan',
            'dateLoading_plan', 'dateReceiptEmpty_plan'
        ]
        numeric_columns: list = [
            'overpayment', 'totalRate', 'amountDowntime', 'rateAgreed',
            'containerSize', 'amountOverload', 'rateCarrier',
            'economy', 'amountAddExpense'
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
        self.removed_columns_rabbit = ['bl']

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
        numeric_columns: list = ['containerTEU', 'containerCount', 'section', 'year', 'month']

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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = [
            'containerSize', 'operationMonth', 'containerCount',
            'containerTEU', 'operationYear'
        ]
        date_columns: list = ['operationDate', 'orderDate']

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
        return None

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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['containerSize', 'containerCount', 'FreightRate']
        date_columns: list = ['cargoReadiness', 'ETD', 'ETA', 'bookingDate']

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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['containerSize', 'containerCount', 'FreightRate']
        date_columns: list = ['ETD', 'ETA', 'bookingDate']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None


class CompletedRepackagesReport(DataCoreClient):
    def __init__(self):
        super().__init__()
        self.removed_columns_rabbit = [
            'bl',
            'export_сontainer_count',
            'import_сontainer_count',
            'сontainer_number',
            'inspection_сontainer_count'
        ]

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
        numeric_columns: list = [
            'warehouse_wms_count', 'inspection_сontainer_count', 'import_teu',
            'import_сontainer_count', 'export_teu', 'export_сontainer_count'
        ]
        date_columns: list = ['repackingDate']

        for column in numeric_columns:
            data[column] = int(data.get(column)) if data.get(column) else None
        for column in date_columns:
            data[column] = self.convert_format_date(data.get(column), data, column) if data.get(column) else None

        data['booking_list'] = data.get('bl')

        # Русская раскладка ↓
        data['inspection_container_count'] = data.get('inspection_сontainer_count') \
            if data.get('inspection_сontainer_count') else data.get('inspection_container_count')
        data['import_container_count'] = data.get('import_сontainer_count') \
            if data.get('import_сontainer_count') else data.get('import_container_count')
        data['container_number'] = data.get('сontainer_number') \
            if data.get('сontainer_number') else data.get('container_number')
        data['export_container_count'] = data.get('export_сontainer_count') \
            if data.get('export_сontainer_count') else data.get('export_container_count')


class AutoVisits(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

    @property
    def deal(self):
        return "queueID"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['processingTime', 'waitingTime']
        date_columns: list = ['exitDate', 'entryDate', 'registrationDate']

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
        return "subjectNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        date_columns: list = ['startDate', 'endDate', 'requestDate']

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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['weightCargo', 'weightTare', 'tonnage', 'containerSize']
        date_columns: list = ['motionDate']

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
        return "orderNumber"

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        numeric_columns: list = ['containerSize', 'operationMonth', 'operationYear']
        date_columns: list = ['operationDate']

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
        return "order_number"

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
        ImportBookings,
        CompletedRepackagesReport,
        AutoVisits,
        AccountingDocumentsRequests,
        DailySummary,
        RZHDOperationsReport,
        OrdersMarginalityReport
    ]
    CLASS_NAMES_AND_TABLES: dict = {
        table_name: class_name
        for table_name, class_name in zip(list(TABLE_NAMES.values()), CLASSES)
    }
    Receive().read_msg()
