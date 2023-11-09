import sys
import time
import json
import contextlib
from app_logger import *
from pathlib import Path
from __init__ import RabbitMq
from datetime import datetime
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
        self.read_text_msg(do_read_file=True)
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
                      f"test_deal.json", 'r') as file:
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
    def convert_format_date(date: str) -> str:
        """operationMonth
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return date

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        pass

    def parse_data(self, data, dat_core: Any) -> str:
        file_name: str = f"data_core_{datetime.now()}.json"
        len_rows: int = len(data)
        for d in data:
            # if len(d) != 24:
            #     raise ValueError(f"The number of columns does not match in {d}")
            self.add_new_columns(len_rows, d, file_name)
            dat_core.change_columns(d)
        self.write_to_json(data)
        return file_name

    def read_json(self, msg: str) -> Tuple[list, str, Any]:
        """
        Decoding a message and working with data.
        :param msg:
        :return:
        """
        msg = msg.decode('utf-8-sig') if isinstance(msg, (bytes, bytearray)) else msg
        all_data: dict = json.loads(msg) if isinstance(msg, str) else msg
        rus_table_name = all_data.get("header", {}).get("report")
        eng_table_name = TABLE_NAMES.get(rus_table_name)
        data = all_data.get("data", [])
        self.logger.info(f'Starting read json. Length of json is {len(data)}')
        CLASS_NAMES_AND_TABLES: dict = {
            LIST_TABLES[0]: CounterParties,
            LIST_TABLES[1]: DataCoreFreight,
            LIST_TABLES[2]: DataCoreSegment
        }
        data_core: Any = CLASS_NAMES_AND_TABLES.get(eng_table_name)
        data_core.table = eng_table_name
        file_name: str = self.parse_data(data, data_core)
        return data, file_name, data_core

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

    @staticmethod
    def write_to_json(msg: str, dir_name: str = "json") -> None:
        """
        Write data to json file
        :param msg:
        :param dir_name:
        :return:
        """
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/{dir_name}/{datetime.now()}.json"
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
        group_list: list = list({dictionary['orderNumber']: dictionary for dictionary in data}.values())
        for item in group_list:
            self.client.query(f"""ALTER TABLE {self.table} 
                        UPDATE is_obsolete=true
                        WHERE original_file_parsed_on != '{file_name}' AND is_obsolete=false 
                        AND orderNumber='{item['orderNumber']}'""")
            self.logger.info("Success updated `is_obsolete` key on `True`")
        self.logger.info("Success updated `is_obsolete` key")

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

    def change_columns(self, data: dict) -> None:
        """
        Changes columns in data.
        :param data:
        :return:
        """
        voyageDate: Optional[str] = data.get('voyageDate')
        operationDate: Optional[str] = data.get('operationDate')
        containerCount: Optional[str] = data.get('containerCount')
        containerSize: Optional[str] = data.get('containerSize')
        voyageMonth: Optional[str] = data.get('voyageMonth')
        operationMonth: Optional[str] = data.get('operationMonth')
        data['voyageDate'] = self.convert_format_date(voyageDate) if voyageDate else None
        data['operationDate'] = self.convert_format_date(operationDate) if operationDate else None
        data['containerCount'] = int(containerCount) if containerCount else None
        data['containerSize'] = int(containerSize) if containerSize else None
        data['voyageMonth'] = datetime.strptime(self.convert_format_date(voyageMonth), "%Y-%m-%d").month \
            if voyageMonth else None
        data['operationMonth'] = int(operationMonth) if operationMonth else None
        data['booking_list'] = data.get('bl')


class DataCoreSegment(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

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
        data['Date'] = self.convert_format_date(date) if date else None


class CounterParties(DataCoreClient):
    def __init__(self):
        super().__init__()

    @property
    def table(self):
        return self.table

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


if __name__ == '__main__':
    Receive().read_msg()
