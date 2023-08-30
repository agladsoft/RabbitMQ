import sys
import time
import uuid
import json
import contextlib
from app_logger import *
from tinydb import Query
from pathlib import Path
from tinydb import TinyDB
from __init__ import RabbitMq
from typing import List, Union
from tinydb.table import Document
from datetime import datetime, timedelta
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driverc.dataconv import Sequence

date_formats: tuple = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z")


class Receive(RabbitMq):
    def __init__(self):
        super().__init__()
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))
        self.db: TinyDB = TinyDB(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/db.json", indent=4, ensure_ascii=False)

    def read_msg(self):
        """
        Connecting to a queue and receiving messages
        :return:
        """
        self.logger.info('Connect')
        channel, connection = self.connect_rabbit()
        channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        channel.queue_declare(queue=self.queue_name,durable=self.durable)
        channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        """
        Working with the message body
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))
        self.logger.info(f"Callback for ch={ch}, method={method}, properties={properties} and body_message called")
        time.sleep(self.time_sleep)
        self.save_text_msg(body)
        self.read_json(body)
        dat_core_client: DataCoreClient = DataCoreClient()
        dat_core_client.main()
        del dat_core_client


    @staticmethod
    def save_text_msg(msg):
        """

        :param msg:
        :return:
        """
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/{datetime.now()}-text_msg.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg.decode('utf-8-sig'), file, indent=4, ensure_ascii=False)

    def change_columns(self, data):
        """

        :param data:
        :return:
        """
        voyageDate = data.get('voyageDate')
        if voyageDate is not None:
            data['voyageDate'] = self.convert_format_date(voyageDate)
        containerCount = data.get('containerCount')
        if containerCount is not None:
            data['containerCount'] = int(containerCount)
        containerSize = data.get('containerSize')
        if containerSize is not None:
            data['containerSize'] = int(containerSize)


    @staticmethod
    def convert_format_date(date: str) -> str:
        """
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return date

    def read_json(self, msg):
        """
        Decoding a message and working with data.
        :param msg:
        :return:
        """
        self.logger.info('Read json')
        msg = msg.decode('utf-8-sig')
        file_name = f"data_core_{datetime.now()}.json"
        data = json.loads(msg)
        for n, d in enumerate(data):
            self.add_new_columns(d, file_name)
            self.change_columns(d)
            self.write_to_json(d, n)
        self.db.insert({"len_rows": len(data), "file_name": file_name, "data": data})
        self.logger.info(f"Data from the queue is written to the cache. File is {file_name}")

    @staticmethod
    def add_new_columns(data, file_name):
        """
        Adding new columns.
        :param data:
        :param file_name:
        :return:
        """
        data['uuid'] = str(uuid.uuid4())
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = None
        data['is_obsolete_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @staticmethod
    def write_to_json(msg, en, dir_name="json"):
        """
        Write data to json file
        :param msg:
        :param en:
        :param dir_name:
        :return:
        """
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/{dir_name}/{en}-{datetime.now()}.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg, file, indent=4, ensure_ascii=False)

    def main(self):
        """

        :return:
        """
        self.logger.info('Start read')
        self.read_msg()
        self.logger.info('End read')


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in Singleton._instances:
            Singleton._instances[cls] = super().__call__(*args, **kwargs)
        return Singleton._instances[cls]


class DataCoreClient(Receive, metaclass=Singleton):
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
                f"SELECT count(*) FROM datacore_freight WHERE original_file_parsed_on='{all_data_cache[-1]['file_name']}'"
        ).result_rows[0][0]:
            self.logger.info("The data has not yet been uploaded to the database")
            time.sleep(60)
        return all_data_cache

    @staticmethod
    def _set_nested(path: list, val: Union[bool, str], condition: str):
        """
        Setting nested data from the cache.
        :param path: Nested data path.
        :param val: The value to replace the old one.
        :param condition: Field from json (the current field is uuid).
        :return:
        """
        def transform(doc):
            list_current = doc
            for key in path[:-1]:
                list_current = list_current[key]

            for current in list_current:
                if condition == current["uuid"]:
                    current[path[-1]] = val

        return transform

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

    def write_updated_data(self, query: str, data_cache: Union[Document, list], is_obsolete: bool) -> None:
        """
        Writing updated data to a json-file.
        :param query: ClickHouse Client.
        :param data_cache: Data from cache.
        :param is_obsolete: Setting the value of is_obsolete.
        :return:
        """
        data_db: Sequence = self.client.query(query).result_rows
        if not data_db:
            return
        date_now: str = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if not is_obsolete:
            for index, row in enumerate(data_cache["data"]):
                self.change_values(is_obsolete, row, date_now)
                self.write_to_json(row, index, dir_name="update")
        else:
            for all_data_cache in data_cache:
                for index, row in enumerate(all_data_cache["data"]):
                    for row_db in data_db:
                        if row["uuid"] == str(row_db[0]):
                            self.change_values(is_obsolete, row, date_now)
                            self.write_to_json(row, index, dir_name="update")
        self.update_cache(data_db, is_obsolete, date_now)

    def update_cache(self, data_db: Sequence, is_obsolete: bool, date_now: str) -> None:
        """
        Updating data from the cache.
        :param data_db: Data from the database.
        :param is_obsolete: Setting the value of is_obsolete.
        :param date_now: write in cache datetime now.
        :return:
        """
        for row in data_db:
            self.db.update(self._set_nested(['data', 'is_obsolete'], is_obsolete, str(row[0])))
            self.db.update(self._set_nested(['data', 'is_obsolete_date'], date_now, str(row[0])))

    def _remove_nested(self, path: list, condition: str):
        """
        Deleting nested data from the cache.
        :param path: Nested data path.
        :param condition: Current date minus the number of days.
        :return:
        """
        def transform(doc):
            list_current = doc
            for key in path[:-1]:
                list_current = list_current[key]

            list_current_copy: list = list_current.copy()
            if not list_current_copy:
                query = Query()
                self.db.remove(query.data == [])
            for current in list_current_copy:
                if condition > current["is_obsolete_date"] and current["is_obsolete"] is True:
                    list_current.remove(current)

        return transform

    def delete_data_from_cache(self) -> None:
        """
        Deleting data from the cache by date.
        :return:
        """
        date: str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.db.remove(self._remove_nested(['data', 'is_obsolete_date'], date))

    def update_status(self, data_cache: Document, all_data_cache) -> None:
        """
        Updating the transaction by parameters.
        :return:
        """
        self.write_updated_data("""
            SELECT *
            FROM datacore_freight
            WHERE is_obsolete is NULL""", data_cache, is_obsolete=False)

        group_list: list = list({dictionary['orderNumber']: dictionary for dictionary in data_cache["data"]}.values())
        for item in group_list:
            self.write_updated_data(f"""SELECT *
                        FROM datacore_freight
                        WHERE original_file_parsed_on != '{data_cache['file_name']}' AND is_obsolete=false 
                        AND orderNumber='{item['orderNumber']}'""", all_data_cache, is_obsolete=True)
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
        for data_cache in all_data_cache_:
            if any(data["is_obsolete"] is None for data in data_cache["data"]):
                self.update_status(data_cache, all_data_cache_)
        self.delete_data_from_cache()

    def __del__(self):
        self.client.close()


if __name__ == '__main__':
    Receive().main()
