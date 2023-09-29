import sys
import time
import uuid
import json
import contextlib
from typing import List
from tinydb import where
from app_logger import *
from pathlib import Path
from itertools import groupby
from __init__ import RabbitMq
from tinydb import TinyDB, Query
from tinydb.table import Document
from datetime import datetime, timedelta
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driverc.dataconv import Sequence

date_formats: tuple = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%d.%m.%Y %H:%M:%S")


class Receive(RabbitMq):
    def __init__(self):
        super().__init__()
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_")
                                                    + str(datetime.now().date()))
        self.db: TinyDB = TinyDB(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/db.json", indent=4, ensure_ascii=False)

    def read_msg(self):
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

    def read_text_msg(self, do_read_file=False):
        """

        :param do_read_file:
        :return:
        """
        if do_read_file:
            with open(f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/"
                      f"2023-09-29 07:31:00.944115-text_msg.json", 'r') as file:
                self.callback(ch='', method='', properties='', body=json.loads(file.read()))

    def callback(self, ch, method, properties, body):
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
        self.read_json(body)
        dat_core_client: DataCoreClient = DataCoreClient()
        dat_core_client.main()
        self.logger.info("Callback exit. The data from the queue was processed by the script")

    @staticmethod
    def save_text_msg(msg):
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

    def change_columns(self, data):
        """
        Changes columns in data.
        :param data:
        :return:
        """
        voyageDate = data.get('voyageDate')
        operationDate = data.get('operationDate')
        containerCount = data.get('containerCount')
        containerSize = data.get('containerSize')
        voyageMonth = data.get('voyageMonth')
        operationMonth = data.get('operationMonth')
        data['voyageDate'] = self.convert_format_date(voyageDate) if voyageDate else None
        data['operationDate'] = self.convert_format_date(operationDate) if operationDate else None
        data['containerCount'] = int(containerCount) if containerCount else None
        data['containerSize'] = int(containerSize) if containerSize else None
        data['voyageMonth'] = datetime.strptime(self.convert_format_date(voyageMonth), "%Y-%m-%d").month \
            if voyageMonth else None
        data['operationMonth'] = int(operationMonth) if operationMonth else None
        data['booking_list'] = data.get('bl')

    @staticmethod
    def convert_format_date(date: str) -> str:
        """operationMonth
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return date

    def parse_data(self, data):
        file_name = f"data_core_{datetime.now()}.json"
        len_rows = len(data)
        for d in data:
            # if len(d) != 24:
            #     raise ValueError(f"The number of columns does not match in {d}")
            self.add_new_columns(len_rows, d, file_name)
            self.change_columns(d)
        self.write_to_json(data)
        return file_name

    def read_json(self, msg):
        """
        Decoding a message and working with data.
        :param msg:
        :return:
        """
        self.logger.info('Read json')
        msg = msg.decode('utf-8-sig') if isinstance(msg, (bytes, bytearray)) else msg
        data = json.loads(msg) if isinstance(msg, str) else msg
        file_name = self.parse_data(data)
        self.db.insert_multiple(iter(data))
        self.logger.info(f"Data from the queue is written to the cache. File is {file_name}")

    @staticmethod
    def add_new_columns(len_rows, data, file_name):
        """
        Adding new columns.
        :param len_rows:
        :param data:
        :param file_name:
        :return:
        """
        data['uuid'] = str(uuid.uuid4())
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = None
        data['is_obsolete_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data['len_rows'] = len_rows

    @staticmethod
    def write_to_json(msg, dir_name="json"):
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

    def get_all_data_db_accord_last_data(self) -> List[Document]:
        """
        Counting the number of rows to update transaction data.
        :return:
        """
        all_data_cache: List[Document] = self.db.all()
        while all_data_cache[-1]["len_rows"] != self.client.query(
                f"SELECT count(*) FROM datacore_freight "
                f"WHERE original_file_parsed_on='{all_data_cache[-1]['original_file_parsed_on']}'"
        ).result_rows[0][0]:
            self.logger.info("The data has not yet been uploaded to the database")
            time.sleep(60)
        self.logger.info("The data has been uploaded to the database")
        return all_data_cache

    @staticmethod
    def change_values(is_obsolete: bool, row: dict, date_now: str) -> None:
        """
        Changing the data in the dictionary for writing to json and then uploading to the database.
        :param is_obsolete: Setting the value of is_obsolete.
        :param row: Dictionary of data.
        :param date_now: write in json-file datetime now.
        :return:
        """
        row["is_obsolete"] = is_obsolete
        row['is_obsolete_date'] = date_now

    def update_cache(self, row_db: Sequence, is_obsolete: bool, date_now: str) -> None:
        """
        Updating data from the cache.
        :param row_db: Data from the database.
        :param is_obsolete: Setting the value of is_obsolete.
        :param date_now: write in cache datetime now.
        :return:
        """
        self.db.update_multiple([
            ({"is_obsolete": is_obsolete}, where('uuid') == str(row_db[0])),
            ({"is_obsolete_date": date_now}, where('uuid') == str(row_db[0])),
        ])

    def delete_data_from_cache(self, query_cache: Query) -> None:
        """
        Deleting data from the cache by date.
        :return:
        """
        date: str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.db.remove(date > query_cache.is_obsolete_date and query_cache.is_obsolete == True)

    def update_status(self, data_cache: dict) -> None:
        """
        Updating the transaction by parameters.
        :return:
        """
        self.client.query(f"""
            ALTER TABLE datacore_freight
            UPDATE is_obsolete=false
            WHERE original_file_parsed_on='{data_cache['file_name']}'
        """)

        group_list: list = list({dictionary['orderNumber']: dictionary for dictionary in data_cache["data"]}.values())
        for item in group_list:
            self.client.query(f"""ALTER TABLE datacore_freight 
                            UPDATE is_obsolete=true
                            WHERE original_file_parsed_on != '{data_cache['file_name']}' 
                            AND is_obsolete=false 
                            AND orderNumber='{item['orderNumber']}'""")
        self.logger.info(f"Success updated `is_obsolete` key. File name is {data_cache['file_name']}.")

    def delete_deal(self) -> None:
        """
        Deleting an is_obsolete key transaction.
        :return:
        """
        self.client.query("DELETE FROM datacore_freight WHERE is_obsolete=true")
        self.logger.info("Successfully deleted old transaction data")

    def main(self):
        """
        The main method that runs the basic functions.
        :return:
        """
        all_data_cache_: List[Document] = self.get_all_data_db_accord_last_data()
        dict_group_by_data: dict = {}
        query_cache: Query = Query()
        for file_name, data in groupby(all_data_cache_, key=lambda x: x['original_file_parsed_on']):
            dict_group_by_data["file_name"] = file_name
            dict_group_by_data["data"] = list(data)
            if any(data["is_obsolete"] is None for data in dict_group_by_data["data"]):
                self.logger.info(f"From this queue, you need to update the values. File name is "
                                 f"{dict_group_by_data['file_name']}")
                self.update_status(dict_group_by_data)
        self.delete_data_from_cache(query_cache)

    def __exit__(self, exception_type, exception_val, trace):
        try:
            self.client.close()
            self.logger.info("Success disconnect clickhouse")
        except AttributeError:  # isn't closable
            self.logger.info("Not closable")
            return True


if __name__ == '__main__':
    Receive().read_msg()
