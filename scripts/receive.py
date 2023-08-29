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


logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))
date_formats: tuple = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z")


class Receive(RabbitMq):
    def __init__(self):
        super().__init__()
        self.db = TinyDB(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/db.json", indent=4, ensure_ascii=False)
        self.client: Client = self.connect_to_db()

    @staticmethod
    def connect_to_db() -> Client:
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            client.query("SET allow_experimental_lightweight_delete=1")
            logger.info("Success connect to clickhouse")
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            sys.exit(1)
        return client

    def read_msg(self):
        ''' Connecting to a queue and receiving messages '''
        logger.info('Connect')
        channel, connection = self.connect_rabbit()
        channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        channel.queue_declare(queue=self.queue_name,durable=self.durable)
        channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        ''' Working with the message body'''
        time.sleep(self.time_sleep)
        logger.info('Get body message')
        self.save_text_msg(body)
        self.read_json(body)
        time.sleep(10)
        DataCoreClient().main()
        # ch.basic_ack(delivery_tag=method.delivery_tag)

    def save_text_msg(self, msg):
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/{datetime.now()}-text_msg.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg.decode('utf-8-sig'), file, indent=4, ensure_ascii=False)

    def change_columns(self, data):
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
        ''' Decoding a message and working with data'''
        logger.info('Read json')
        msg = msg.decode('utf-8-sig')
        file_name = f"data_core_{datetime.now()}.json"
        data = json.loads(msg)
        list_uuids: List[str] = self.get_list_from_query()
        for n, d in enumerate(data):
            self.add_new_columns(d, file_name, list_uuids)
            self.change_columns(d)
            self.write_to_json(d, n)
        self.db.insert({"len_rows": len(data), "file_name": file_name, "data": data})

    def get_list_from_query(self) -> list:
        """
        Get all uuid values from the database in order not to write the same uuid.
        :return:
        """
        try:
            return [str(uuid_db[0]) for uuid_db in self.client.query("SELECT uuid FROM datacore_freight").result_rows]
        except Exception as ex_connect:
            logger.error(f"Failed to connect to the database and get all the uuid values. Exception is {ex_connect}")
            return []

    @staticmethod
    def add_new_columns(data, file_name, list_uuids):
        """
        Adding new columns.
        :param data:
        :param file_name:
        :param list_uuids:
        :return:
        """
        uuid_gen: str = str(uuid.uuid4())
        while uuid_gen in list_uuids:
            uuid_gen = str(uuid.uuid4())
        list_uuids.append(uuid_gen)
        data['uuid'] = uuid_gen
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = None
        data['is_obsolete_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, msg, en, dir_name="json"):
        ''' Write data to json file '''
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/{dir_name}/{en}-{datetime.now()}.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg, file, indent=4, ensure_ascii=False)

    def main(self):
        logger.info('Start read')
        self.read_msg()
        logger.info('End read')


class DataCoreClient(Receive):
    def __init__(self):
        super().__init__()

    def get_all_data_db_accord_last_data(self) -> List[Document]:
        """
        Counting the number of rows to update transaction data.
        :return:
        """
        all_data_cache: List[Document] = self.db.all()
        while all_data_cache[-1]["len_rows"] != self.client.query(
                f"SELECT count(*) FROM datacore_freight WHERE original_file_parsed_on='{all_data_cache[-1]['file_name']}'"
        ).result_rows[0][0]:
            logger.info("The data has not yet been uploaded to the database")
            time.sleep(60)
        return all_data_cache

    @staticmethod
    def set_nested(path: list, val: Union[bool, str], condition: str):
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
            self.db.update(self.set_nested(['data', 'is_obsolete'], is_obsolete, str(row[0])))
            self.db.update(self.set_nested(['data', 'is_obsolete_date'], date_now, str(row[0])))

    def remove_nested(self, path: list, condition: str):
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
        self.db.remove(self.remove_nested(['data', 'is_obsolete_date'], date))

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
        logger.info("Success updated `is_obsolete` key")

    def delete_deal(self) -> None:
        """
        Deleting an is_obsolete key transaction.
        :return:
        """
        self.client.query("DELETE FROM datacore_freight WHERE is_obsolete=true")
        logger.info("Successfully deleted old transaction data")

    def main(self):
        """
        The main method that runs the basic functions.
        :return:
        """
        all_data_cache_: List[Document] = self.get_all_data_db_accord_last_data()
        self.update_status(all_data_cache_[-1], all_data_cache_)
        self.delete_data_from_cache()


if __name__ == '__main__':
    Receive().main()
