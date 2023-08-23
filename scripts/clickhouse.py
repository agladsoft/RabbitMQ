import sys
import time
from app_logger import *
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client


class ClickHouse:
    def __init__(self, logger: logging.getLogger):
        self.logger: logging.getLogger = logger
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
            print("error_connect_db", file=sys.stderr)
            sys.exit(1)
        return client

    def count_number_loaded_rows(self, data: list, count_rows: int, file_name: str) -> None:
        """
        Counting the number of rows to update transaction data.
        :param data: Data from RabbitMQ.
        :param count_rows: Count rows of db.
        :param file_name: Name of file.
        :return:
        """
        while count_rows != self.client.query(
                f"SELECT count(*) FROM datacore_freight WHERE original_file_parsed_on='{file_name}'"
        ).result_rows[0][0]:
            time.sleep(10)
        self.update_deal(data, file_name)

    def update_deal(self, data: list, file_name: str) -> None:
        """
        Updating the transaction by parameters.
        :param data: Data from RabbitMQ.
        :param file_name: Name of file.
        :return:
        """
        group_list: list = list({dictionary['orderNumber']: dictionary for dictionary in data}.values())
        for item in group_list:
            self.client.query(f"""ALTER TABLE datacore_freight 
                UPDATE is_obsolete=true
                WHERE original_file_parsed_on != '{file_name}' AND is_obsolete=false AND orderNumber='{item['orderNumber']}'""")
        self.client.query(f"""
            ALTER TABLE datacore_freight
            UPDATE is_obsolete=false
            WHERE original_file_parsed_on='{file_name}'
        """)
        self.logger.info("Success updated `is_obsolete` key")

    def delete_deal(self) -> None:
        """
        Deleting an is_obsolete key transaction.
        :return:
        """
        self.client.query("DELETE FROM datacore_freight WHERE is_obsolete=true")
        self.logger.info("Successfully deleted old transaction data")
