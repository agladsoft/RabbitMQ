import sys
import time
from app_logger import *
from tinydb import Query
from receive import Receive
from typing import List, Union
from tinydb.table import Document
from datetime import datetime, timedelta
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driverc.dataconv import Sequence


logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))
date_formats: tuple = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z")


class DataCoreClient(Receive):
    def __init__(self):
        super().__init__()
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
            print("error_connect_db", file=sys.stderr)
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
            logger.info("The data has not yet been uploaded to the database")
            time.sleep(60)
        return all_data_cache

    @staticmethod
    def set_nested(path: list, val: Union[bool, str], condition: str):
        """
        Deleting nested data from the cache.
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

    def delete_data_from_cache(self):
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
        self.write_updated_data(f"""
            SELECT *
            FROM datacore_freight
            WHERE original_file_parsed_on='{data_cache['file_name']}' AND is_obsolete is NULL""", data_cache, is_obsolete=False)

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


if __name__ == "__main__":
    DataCoreClient().main()
