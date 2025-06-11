import re
import pytz
import json
import contextlib
from datetime import datetime, date, timedelta
from scripts.__init__ import LOG_TABLE, BATCH_SIZE
from clickhouse_connect.driver.query import QueryResult
from typing import TYPE_CHECKING, Tuple, Union, Optional, List, Any

if TYPE_CHECKING:
    from scripts.receive import Receive

DATE_FORMATS: tuple = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%d.%m.%YT%H:%M:%SZ",
    "%d.%m.%YT%H:%M:%S",
    "%d.%m.%YT%H:%M:%S%z",
    "%d.%m.%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%d.%m.%Y",
    "%Y-%m-%d"
)
TZ: pytz.timezone = pytz.timezone("Europe/Moscow")


def serialize_datetime(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")


class DataCoreClient:
    def __init__(self, receive: "Receive"):
        self.receive: "Receive" = receive
        self.removed_columns_db: list = ['uuid']
        self._original_date_string: Optional[str] = None

    @property
    def database(self):
        return "DataCore"

    @property
    def deal(self):
        return "key_id"

    @property
    def table(self):
        return self.table

    @table.setter
    def table(self, value: str):
        self.table: str = value

    @property
    def original_date_string(self):
        return self._original_date_string

    @original_date_string.setter
    def original_date_string(self, value):
        self._original_date_string: str = value

    def change_columns(self, data: dict, *args, **kwargs) -> None:
        """
        Changes data types of columns in data dictionary according to given parameters.

        The following parameters can be passed in the kwargs:

        - float_columns: list of columns to be converted to float
        - int_columns: list of columns to be converted to int
        - date_columns: list of columns to be converted to datetime
        - bool_columns: list of columns to be converted to bool
        - is_datetime: boolean indicating whether date_columns should be converted to datetime or date

        The method processes all columns in the given lists and replaces the values in the data dictionary
        with the converted values. If a value in the data dictionary is None, it is left unchanged.

        :param data: dictionary to be modified
        :param args: not used
        :param kwargs: parameters to control the conversion of columns
        :return:
        """
        float_columns: list = kwargs.get('float_columns', [])
        int_columns: list = kwargs.get('int_columns', [])
        date_columns: list = kwargs.get('date_columns', [])
        bool_columns: list = kwargs.get('bool_columns', [])
        is_datetime: bool = kwargs.get('is_datetime', False)

        for column in float_columns:
            data[column] = float(
                re.sub(r'(?<=\d)\s+(?=\d)', '', str(data.get(column))).replace(",", ".")
            ) if data.get(column) else None

        for column in int_columns:
            data[column] = int(
                re.sub(r'(?<=\d)\s+(?=\d)', '', str(data.get(column)))
            ) if data.get(column) else None

        for column in date_columns:
            data[column] = self.convert_format_date(
                data.get(column), data, column, is_datetime=is_datetime
            ) if data.get(column) else None

        for column in bool_columns:
            if isinstance(data.get(column), str):
                data[column] = data.get(column).upper() == 'ДА'

    def convert_format_date(self, date_: str, data: dict, column, is_datetime: bool = False) -> Union[datetime, str]:
        """
        Converts a date string from a file to a datetime object.

        The function loops over the list of date formats in the DATE_FORMATS constant,
        and for each format, it tries to parse the date string using the datetime.strptime
        function. If the parsing is successful, it checks if the parsed date is less than
        the date of "1925-01-01". If it is, it adds the parsed date to the value of the
        original_date_string key in the data dictionary, and returns the date of
        "1925-01-01". If the parsed date is not less than "1925-01-01", it returns the
        parsed date.

        If the date string cannot be parsed using any of the formats in the DATE_FORMATS
        constant, the function returns the original date string.

        :param date_: The date string to be parsed.
        :param data: A dictionary with data to be processed.
        :param column: The name of the column in the data dictionary.
        :param is_datetime: A boolean indicating whether the date string represents a datetime or a date.
        :return: A datetime object or the original date string if it cannot be parsed.
        """
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                if not is_datetime:
                    date_file: Union[datetime.date, datetime] = datetime.strptime(date_, date_format).date()
                    date_db_access: Union[datetime.date, datetime] = datetime.strptime("1925-01-01", "%Y-%m-%d").date()
                else:
                    utc: pytz.timezone = pytz.utc
                    date_file = utc.localize(datetime.strptime(date_, date_format))
                    date_db_access = utc.localize(datetime.strptime("1925-01-01", "%Y-%m-%d"))

                if date_file < date_db_access:
                    data[self.original_date_string] += f"({column}: {date_file})\n"
                    return date_db_access
                return date_file
        return date_

    @staticmethod
    def add_new_columns(data: dict, file_name: str, original_date_string: str) -> None:
        """
        Adds new columns to the data dictionary.

        The function adds three new columns to the data dictionary. The first column is
        called 'sign' and is set to 1. The second column is called 'original_file_parsed_on'
        and is set to the file name of the file being processed. The third column is called
        'is_obsolete_date' and is set to the current date and time in the "Europe/Moscow"
        time zone. If the original_date_string parameter is not empty, the function also
        adds an empty string to the column with the name given by the parameter.

        :param data: A dictionary with data to be processed.
        :param file_name: The name of the file being processed.
        :param original_date_string: The name of the column to be added with an empty string.
        :return:
        """
        data['sign'] = 1
        data['original_file_parsed_on'] = file_name
        data['is_obsolete_date'] = datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S")
        if original_date_string:
            data[original_date_string] = ''

    @staticmethod
    def convert_to_lowercase(data: dict):
        """
        Converts all keys in the given dictionary to lowercase.

        The function takes in a dictionary and returns a new dictionary where all the keys
        are converted to lowercase using the str.lower() method.

        :param data: A dictionary with keys to be converted to lowercase.
        :return: A new dictionary with lowercase keys.
        """
        return {k.lower(): v for k, v in data.items()}

    def check_difference_columns(
        self,
        all_data: dict,
        list_columns_db: list,
        list_columns_rabbit: list,
        key_deals: str,
        message_count: int
    ) -> list:
        """
        Checks the difference in columns between the database and RabbitMQ message.

        The method returns a list of columns which are not present in either the database or
        the RabbitMQ message. If there are differences in columns, it logs an error message,
        adds the key deals to the message errors list, writes the data to an error file, and
        inserts a message into the database with the is_success_inserted flag set to False.
        If there are no differences in columns, it returns an empty list.

        :param all_data: A dictionary with data to be processed.
        :param list_columns_db: A list of column names present in the database.
        :param list_columns_rabbit: A list of column names present in the RabbitMQ message.
        :param key_deals: A string identifier for key deals.
        :param message_count: The count of messages in the queue.
        :return: A list of columns which are not present in either the database or the RabbitMQ message.
        """
        diff_db: list = list(set(list_columns_db) - set(list_columns_rabbit))
        diff_rabbit: list = list(set(list_columns_rabbit) - set(list_columns_db))
        if diff_db or diff_rabbit:
            self.receive.logger.error(
                f"The difference in columns {diff_db} from the database. "
                f"The difference in columns {diff_rabbit} from the rabbit"
            )
            self.insert_message(all_data, key_deals, message_count, is_success_inserted=False)
            return diff_db + diff_rabbit
        return []

    def get_table_columns(self):
        """
        Retrieves the column names of the specified table in the database.

        This method queries the database to describe the structure of the table
        and extracts the list of column names from the result.

        :return: A list of column names in the specified table.
        """
        described_table = self.receive.client.query(f"DESCRIBE TABLE {self.database}.{self.table}")
        return described_table.result_columns[0]

    def insert_message(
        self,
        all_data: dict,
        key_deals: str,
        message_count: int,
        is_success_inserted: bool
    ) -> None:
        """
        Inserts a message into the ClickHouse database.

        This method takes a dictionary of data, a key deals identifier, the message count,
        and a boolean indicating whether the message was successfully inserted into the
        database. It logs the process and handles any exceptions that occur during data
        insertion.

        :param all_data: A dictionary with data to be logged.
        :param key_deals: A string identifier for key deals.
        :param message_count: The count of messages in the queue.
        :param is_success_inserted: A boolean indicating whether the message was successfully inserted.
        :return None:
        """
        len_data: int = 100
        all_data["data"] = all_data["data"][:len_data] if len(all_data["data"]) >= len_data else all_data["data"]
        row = [
            self.database,
            self.table,
            self.receive.queue_name,
            key_deals,
            datetime.now(tz=TZ) + timedelta(hours=3),
            is_success_inserted,
            json.dumps(all_data, default=serialize_datetime, ensure_ascii=False, indent=2)
        ]
        self.receive.log_message_buffer.append(row)
        if len(self.receive.rows_buffer) >= BATCH_SIZE or message_count == 0 or not is_success_inserted:
            self.receive.client.insert(
                table=LOG_TABLE,
                database="DataCore",
                data=self.receive.log_message_buffer,
                column_names=["database", "table", "queue", "key_id", "datetime", "is_success", "message"]
            )
            self.receive.rows_buffer = []
            self.receive.log_message_buffer = []

    def handle_rows(self, all_data, data: list, key_deals: str, message_count: int) -> None:
        """
        Handles a list of rows to be inserted into the ClickHouse database.

        This method takes a list of rows, converts them to the required format,
        and inserts them into the specified table in the ClickHouse database.

        It also logs the process and handles any exceptions that occur during data
        insertion. If an exception occurs, it writes the data to an error file and
        logs the error.

        :param all_data: A dictionary containing the entire dataset to be processed.
        :param data: A list of data rows to be processed.
        :param key_deals: A string identifier for key deals.
        :param message_count: Queue message count.
        :return: None
        """
        try:
            self.receive.key_deals_buffer.append(key_deals)
            for row in data:
                self.receive.rows_buffer.append(row)
            if len(self.receive.rows_buffer) >= BATCH_SIZE or message_count == 0:
                self.update_status()
                columns, deduped_buffer = self.dedupe_rows_buffer()
                if columns and deduped_buffer:
                    self.receive.client.insert(
                        table=self.table,
                        database=self.database,
                        data=deduped_buffer,
                        column_names=columns
                    )
                self.receive.rabbit_mq.channel.basic_ack(delivery_tag=self.receive.delivery_tags[-1], multiple=True)
                self.receive.key_deals_buffer = []
                self.receive.delivery_tags = []
                self.receive.logger.info("The data has been uploaded to the database")
            self.insert_message(all_data, key_deals, message_count, is_success_inserted=True)
        except Exception as ex:
            self.receive.logger.error(f"Exception is {ex}. Type of ex is {type(ex)}")
            self.insert_message(all_data, key_deals, message_count, is_success_inserted=False)
            raise ConnectionError(ex) from ex

    def dedupe_rows_buffer(self) -> Tuple[list, List[list]]:
        """
        Deduplicates the rows buffer using the key_id and original_file_parsed_on columns.

        We iterate over the buffer in reverse order, so the most recent rows are checked first.
        If a row's key_id is not in the seen_keys dictionary, we add it to the dictionary and
        append the row to the deduped_buffer list. If the key_id is already in the dictionary,
        we check if the parsed_on date matches the one in the dictionary. If it does, we append
        the row to the deduped_buffer list as well. Finally, we reverse the deduped_buffer list
        to maintain the original order of the rows.

        :return: A list of deduplicated rows and columns
        """
        deduped_buffer: list = []
        seen_keys: dict = {}
        columns = list(self.receive.rows_buffer[0].keys()) if self.receive.rows_buffer else []
        for row in reversed(self.receive.rows_buffer):  # Итерируемся с конца
            key: str = row['key_id']
            parsed_on: str = row['original_file_parsed_on']

            if key not in seen_keys:
                seen_keys[key] = parsed_on
                deduped_buffer.append(list(row.values()))
            elif parsed_on == seen_keys[key]:
                deduped_buffer.append(list(row.values()))  # Добавляем дубликат, если обе даты совпадают

        return columns, list(reversed(deduped_buffer))

    def update_status(self) -> None:
        """
        Updates the status of the records in the database.

        If the key_deals_buffer is not empty, it will be chunked into smaller pieces
        and the database will be queried for each chunk. The results of the query
        will be inserted back into the database with the sign column set to -1.

        :return: None
        """
        if not self.receive.key_deals_buffer:
            return

        chunk_size: int = 1000
        for i in range(0, len(self.receive.key_deals_buffer), chunk_size):
            chunk: list = self.receive.key_deals_buffer[i:i + chunk_size]
            placeholders: str = ', '.join([f"'{key}'" for key in chunk])
            query: str = (
                f"SELECT * FROM {self.database}.{self.table} WHERE uuid IN ("
                f"SELECT uuid FROM {self.database}.{self.table} WHERE {self.deal} IN ({placeholders}) "
                f"GROUP BY uuid HAVING SUM(sign) > 0"
                f")"
            )
            selected_query: QueryResult = self.receive.client.query(query)
            if rows_query := selected_query.result_rows:
                rows_buffer: list = []
                columns: tuple = selected_query.column_names
                for row in rows_query:
                    modified_row: dict = {col: -1 if col == 'sign' else val for val, col in zip(row, columns)}
                    rows_buffer.append(list(modified_row.values()))
                self.receive.client.insert(
                    table=self.table,
                    database=self.database,
                    data=rows_buffer,
                    column_names=columns
                )
            self.receive.logger.info("Data processing in the database is completed")

    def delete_old_deals(self, cond: str = "is_obsolete=true") -> None:
        """
        Deletes all rows from the database table where the condition given in the `cond` parameter is met.

        The default condition is `is_obsolete=true`.
        This method is usually used to remove obsolete deals from the database.

        :param cond: A condition string that is used to filter the rows to be deleted from the table.
        :return:
        """
        self.receive.client.query(f"DELETE FROM {self.database}.{self.table} WHERE {cond}")
        self.receive.logger.info(f"Successfully deleted old transaction data for table {self.database}.{self.table}")

    def __exit__(self, exception_type: Any, exception_val: Any, trace: Any) -> Optional[bool]:
        """
        Handles the exit of the context manager, ensuring proper disconnection
        from the ClickHouse client.

        This method attempts to close the connection to the ClickHouse database
        and logs the success of the disconnection. In the event that the client
        is not closable, an AttributeError is caught and logged accordingly.

        :param exception_type: The type of exception raised (if any).
        :param exception_val: The value of the exception raised (if any).
        :param trace: The traceback of the exception raised (if any).
        :return: None if the client is closed successfully, True if the client is not closable.
        """
        try:
            self.receive.client.close()
            self.receive.logger.info("Success disconnect clickhouse")
            return None
        except AttributeError:  # isn't closable
            self.receive.logger.info("Not closable")
            return True


class DataCoreFreight(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_voyage_month_string"

    def change_columns(self, *args, **kwargs) -> None:
        data: dict = kwargs.get('data')
        super().change_columns(
            data=data,
            float_columns=kwargs.get('float_columns', []),
            int_columns=['container_count', 'container_size', 'operation_month'],
            date_columns=['voyage_date', 'operation_date', 'voyage_month'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

        data['voyage_month'] = data['voyage_month'].month if data.get('voyage_month') else None

    def get_table_columns(self):
        return [
            "key_id", "uuid", "debit_status", "invoice_status", "invoice_port", "terminal", "operation_date",
            "original_voyage_month_string", "voyage", "voyage_month", "voyage_date", "is_freight", "operation_month",
            "container_count", "discharge_load_port", "booking_discharge_port", "booking_load_port", "tnved",
            "container_size", "cargo", "client_inn", "manager", "line", "client", "client_uid", "operation_segment",
            "vessel", "department", "container", "direction", "order_number", "container_type", "consignment",
            "destination_port", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class NaturalIndicatorsContractsSegments(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['year', 'month'],
            date_columns=['date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "direction", "month", "year", "segment", "date", "original_date_string", "order_number",
            "container_number", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class CounterParties(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    def get_table_columns(self):
        return [
            "key_id", "uuid", "main_manager", "classification", "counterparty", "is_supplier", "registration_country",
            "inn", "relationship_type", "legal_physical_entity", "head_counterparty", "full_name", "is_foreign_company",
            "short_name", "is_client", "legal_address", "planned_turnover", "status", "rc_uid", "is_other",
            "counterparty_type", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class OrdersReport(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_voyage_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=kwargs.get('int_columns', []),
            date_columns=['voyage_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "tnved", "line", "load_port", "shipper_inn", "consignee", "shipper", "container_number",
            "vessel", "voyage_date", "voyage", "consignment", "order_number", "original_voyage_date_string",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class AutoPickupGeneralReport(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_date_delivery_plan_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=[
                'overpayment', 'downtime_amount', 'agreed_rate',
                'total_rate', 'carrier_rate', 'economy',
                'overload_amount', 'add_expense_amount'
            ],
            int_columns=['container_size'],
            date_columns=[
                'date_delivery_empty_fact', 'date_delivery_empty_plan', 'date_loading_fact',
                'date_delivery_fact', 'date_receiving_empty_fact', 'date_delivery_plan',
                'date_loading_plan', 'date_receiving_empty_plan'
            ],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "comment", "performer", "overpayment", "total_rate", "downtime_amount", "agreed_rate",
            "carrier", "date_delivery_empty_fact", "original_date_delivery_plan_string", "date_delivery_empty_plan",
            "date_loading_fact", "mode", "date_delivery_fact", "container_size", "overload_amount", "line", "client",
            "client_uid", "date_receiving_empty_fact", "department", "date_delivery_plan", "destination_point",
            "manager", "carrier_rate", "departure_point", "terminal_receiving_empty", "container_number",
            "terminal_delivery_empty", "date_loading_plan", "direction", "container_type", "transportation_type",
            "economy", "order_number", "increased_rates_reason", "service", "date_receiving_empty_plan",
            "add_expense_amount", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class TransportUnits(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    def get_table_columns(self):
        return [
            "key_id", "uuid", "owner", "container_number", "container_type", "container_size",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class Consignments(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_voyage_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['container_size', 'teu', 'year'],
            date_columns=['voyage_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "direction", "year", "voyage_date", "original_voyage_date_string", "voyage", "cargo",
            "container_number", "container_size", "agent_line", "line", "teu", "consignee", "shipper", "order_number",
            "container_type", "consignment", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class SalesPlan(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['teu', 'container_count', 'container_size', 'year', 'month'],
            date_columns=kwargs.get('date_columns', []),
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "teu", "container_count", "container_size", "direction", "client", "year", "month",
            "client_uid", "department", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class NaturalIndicatorsTransactionFactDate(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_operation_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=[
                'container_size', 'operation_month', 'container_count',
                'teu', 'operation_year'
            ],
            date_columns=['operation_date', 'order_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "container_size", "operation_date", "is_full", "original_operation_date_string",
            "operation_month", "container_count", "teu", "manager", "container_type", "operation_year", "order_number",
            "direction", "client", "order_date", "client_uid", "department", "original_file_parsed_on", "sign",
            "is_obsolete_date"
        ]


class DevelopmentCounterpartyDepartment(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['year'],
            date_columns=kwargs.get('date_columns', []),
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "client", "year", "direction", "client_uid", "department", "original_file_parsed_on",
            "sign", "is_obsolete_date"
        ]


class ExportBookings(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_booking_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['container_size', 'container_count', 'freight_rate', 'teu'],
            date_columns=['cargo_readiness', 'etd', 'eta', 'booking_date', 'sob'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "freight_rate_uid", "booking_status", "discharge_port_bay", "client_uid", "client",
            "container_owner", "load_port_bay", "cargo_readiness", "etd", "sob", "teu", "freight_terms",
            "container_number", "container_size", "container_type", "eta", "original_booking_date_string",
            "voyage", "container_count", "discharge_port", "load_port", "direction", "freight_rate",
            "forwarder", "line", "service_number", "department", "vessel", "order_number", "booking_date",
            "manager", "booking", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class ImportBookings(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_booking_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=['freight_rate'],
            int_columns=['container_size', 'container_count', 'teu'],
            date_columns=['etd', 'eta', 'booking_date', 'sob'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "freight_rate_uid", "booking_status", "discharge_port_bay", "client_uid", "client",
            "container_owner", "load_port_bay", "etd", "agent", "container_number", "container_size", "container_type",
            "eta", "sob", "original_booking_date_string", "teu", "freight_terms", "voyage", "container_count",
            "discharge_port", "load_port", "direction", "freight_rate", "forwarder", "remarks", "line", "consignee",
            "service_number", "department", "vessel", "order_number", "booking_date", "manager", "booking",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class CompletedRepackagesReport(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_repacking_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=[
                'warehouse_wms_count', 'inspection_container_count', 'import_teu',
                'import_container_count', 'export_teu', 'export_container_count'
            ],
            date_columns=['repacking_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "warehouse_wms_count", "inspection_container_count", "import_teu",
            "consignment", "import_container_count", "terminal", "export_teu", "zatarka_object_type",
            "rastarka_object_type", "repacking_place", "operation_type", "container_number",
            "export_container_count", "cargo", "technology", "repacking_date",
            "original_repacking_date_string", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class AutoVisits(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_entry_datetime_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['processing_time', 'waiting_time'],
            date_columns=['exit_datetime', 'entry_datetime', 'registration_datetime'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=True
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "processing_place", "is_manual_select", "rejection_reason", "comment",
            "processing_time", "exit_datetime", "entry_datetime", "original_entry_datetime_string",
            "result", "waiting_time", "status", "container_number", "purpose", "registration_datetime",
            "repacking_place", "terminal", "car_number", "request_number", "queue_id",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class AccountingDocumentsRequests(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_request_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=kwargs.get('int_columns', []),
            date_columns=['start_date', 'end_date', 'request_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=True
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "rejection_reason", "status", "performer", "comment",
            "start_date", "manager", "description", "department", "is_urgency",
            "addressee_peo", "end_date", "request_date", "original_request_date_string",
            "order_number", "request_type", "request_number", "original_file_parsed_on",
            "sign", "is_obsolete_date"
        ]


class DailySummary(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_motion_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=['tonnage', 'cargo_weight', 'tare_weight'],
            int_columns=['container_size'],
            date_columns=['motion_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=True
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "organization_uid", "comment", "direction", "cargo_weight",
            "consignment_imp", "vtt_gtd", "cargo", "destination_point", "transport_type",
            "client_uid", "terminal", "motion_type", "seal", "manager", "departure_point",
            "transport_number", "tare_weight", "organization", "tonnage", "is_so", "client",
            "line", "order_number", "container_type", "container_size", "consignment_exp",
            "container_state", "container_number", "forwarder", "damages_note", "motion_date",
            "original_motion_date_string", "is_request_line", "original_file_parsed_on",
            "sign", "is_obsolete_date"
        ]


class RZHDOperationsReport(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_operation_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['container_size', 'operation_month', 'operation_year'],
            date_columns=[
                'operation_date', 'departure_date', 'arrival_date', 'planned_start_date',
                'planned_end_date', 'fact_start_date', 'fact_end_date'
            ],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "document_number", "etsng_code_ktk", "etsng_order_base_cargo", "etsng_operation_cargo",
            "is_border_crossing_point", "service", "client_uid", "client", "etsng_name", "container_size",
            "operation_month", "operation_date", "original_operation_date_string", "direction", "type_of_relation",
            "department", "container_number", "etsng_code", "destination_point", "destination_station_code",
            "destination_point_type", "manager", "client_inn", "departure_point", "departure_station_code",
            "departure_point_type", "order_number", "operation_name", "container_type", "operation_year",
            "departure_date", "arrival_date", "planned_start_date", "planned_end_date", "fact_start_date",
            "fact_end_date", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class OrdersMarginalityReport(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_order_creation_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=[
                'expenses_rental_without_vat_fact', 'income_without_vat_fact', 'profit_plan',
                'income_without_vat_plan', 'expenses_without_vat_plan', 'expenses_without_vat_fact',
                'profit_fact'
            ],
            int_columns=kwargs.get('int_columns', []),
            date_columns=['order_creation_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "expenses_rental_without_vat_fact", "income_without_vat_fact", "profit_plan",
            "income_without_vat_plan", "expenses_without_vat_plan", "manager", "organization_uid", "segment_decryption",
            "organization", "client_uid", "expenses_without_vat_fact", "department", "client", "segment",
            "order_number", "profit_fact", "order_creation_date", "original_order_creation_date_string",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class NaturalIndicatorsRailwayReceptionDispatch(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['container_size', 'container_count', 'teu', 'internal_customs_transit'],
            date_columns=['date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            'key_id', 'uuid', 'date', 'original_date_string', 'organization', 'terminal',
            'client_uid', 'client', 'operation', 'is_empty', 'container_count', 'container_size',
            'teu', 'internal_customs_transit', 'other_transportation', 'container_train',
            'wagon_dispatch_count', 'original_file_parsed_on', 'sign', 'is_obsolete_date'
        ]


class Accounts(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=['profit_account_rub', 'profit_account'],
            int_columns=kwargs.get('int_columns', []),
            date_columns=['date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "date", "original_date_string", "organization", "client_uid", "client", "department",
            "currency", "profit_account_rub", "profit_account", "client_inn", "unit_of_measurement", "order_number",
            "segment", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class FreightRates(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=['rate'],
            int_columns=['oversized_width', 'oversized_height', 'oversized_length'],
            date_columns=['expiration_date', 'start_date'],
            bool_columns=['priority', 'oversized', 'dangerous', 'special_rate', 'guideline'],
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "freight_rate_uid", "priority", "oversized_width", "oversized_height", "oversized_length",
            "oversized", "dangerous", "client", "imo", "pol", "pod", "type_pol", "type_pod", "expiration_date",
            "start_date", "original_date_string", "forwarder", "guideline", "through_service", "rate_owner", "line",
            "size_and_type", "rate", "currency", "special_rate", "code_special_rate", "operator", "container_owner",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class MarginalityOrdersActDate(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_act_creation_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=[
                'profit_plan', 'variable_costs_plan', 'margin_plan',
                'profit_fact', 'variable_costs_fact', 'margin_fact',
                'margin_fact_percent', 'margin_fact_per_unit'
            ],
            int_columns=['count_ktk_by_order', 'count_ktk_by_operation'],
            date_columns=['act_creation_date', 'act_creation_date_max'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "act_creation_date", "act_creation_date_max", "original_act_creation_date_string",
            "department", "direction", "client_uid", "client", "manager", "operation_type", "segment", "order_number",
            "count_ktk_by_order", "count_ktk_by_operation", "profit_plan", "variable_costs_plan", "margin_plan",
            "profit_fact", "variable_costs_fact", "margin_fact", "margin_fact_percent", "margin_fact_per_unit",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class RusconProducts(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_kp_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=[
                'kp_amount', 'kp_margin', 'kp_margin_amount', 'kp_margin_container',
                'kp_amount_cost', 'kp_revenue_rate_container', 'kp_cost_container'
            ],
            int_columns=['container_count_40', 'container_count_20', 'container_count'],
            date_columns=['kp_date'],
            bool_columns=['dangerous'],
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "product", "stepname", "department", "key", "manager_division", "manager", "counterparty",
            "crm_counterparty_number", "project_number", "kp_number", "kp_date", "direction", "container_size",
            "container_type", "container_count", "container_count_40", "container_count_20", "cargo", "kp_currency",
            "kp_amount", "kp_amount_cost", "kp_margin", "kp_margin_amount", "kp_margin_container",
            "kp_revenue_rate_container", "kp_cost_container", "discharge_point", "loading_point", "dangerous",
            "original_kp_date_string", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class ReferenceLocations(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=['lat_port', 'long_port'],
            int_columns=kwargs.get('int_columns', []),
            date_columns=kwargs.get('date_columns', []),
            bool_columns=['is_border_crossing'],
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "seaport", "seaport_eng", "seaport_code", "seaport_rus", "key", "type", "status",
            "postcode", "lat_port", "long_port", "country", "code", "code_alpha_two", "station_code",
            "is_border_crossing", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class TerminalsCapacity(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['container_size', 'teu', 'container_count'],
            date_columns=['date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "stock", "terminal", "date", "original_date_string", "container_size", "container_count",
            "teu", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class ManagerEvaluation(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def database(self):
        return "DO"

    @property
    def original_date_string(self):
        return "original_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=['evaluation'],
            date_columns=['evaluation_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "manager_id", "manager_fullname", "manager_email", "manager_position", "pluralist",
            "manager_division", "manager_department", "manager_status", "portal_profile", "evaluation", "comment",
            "evaluation_date", "original_date_string", "company_email", "message_id", "original_file_parsed_on", "sign",
            "is_obsolete_date"
        ]


class ReferenceCounterparties(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def database(self):
        return "DO"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=kwargs.get('int_columns', []),
            date_columns=kwargs.get('date_columns', []),
            bool_columns=['is_control', 'is_foreign_company'],
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "name", "do_uid", "status", "head_counterparty", "included_in_group",
            "legal_physical_entity", "full_name", "inn", "kpp", "okpo", "ogrn", "is_foreign_company",
            "deletion_flag", "registration_country", "registration_number", "tax_number", "counterparty_type",
            "supplier_customer_type", "classification", "contract_type", "relationship_type", "planned_turnover",
            "manager", "legal_address", "actual_address", "postal_address", "telephone_number", "email", "website",
            "organization_uid", "organization", "is_control", "original_file_parsed_on", "sign", "is_obsolete_date"
        ]


class ReferenceContracts(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def database(self):
        return "DO"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=kwargs.get('float_columns', []),
            int_columns=kwargs.get('int_columns', []),
            date_columns=[
                'contract_date', 'date_of_creation', 'approvals_date', 'signing_date',
                'returned_archive_date', 'date_tripartite_agreement'
            ],
            bool_columns=['returned_archive', 'additional_agreement'],
            is_datetime=kwargs.get('is_datetime', False)
        )

    def get_table_columns(self):
        return [
            "key_id", "uuid", "do_uid", "heading", "template_name", "contract_number", "contract_date",
            "type_of_relationship", "additional_agreement", "organization_uid", "organization_inn", "organization_name",
            "counterparty_uid", "counterparty_inn", "counterparty_name", "prepared_uid", "prepared_name",
            "prepared_cfo", "responsible_uid", "responsible_name", "responsible_cfo", "document_form",
            "date_of_creation", "approval_status", "approvals_date", "signing_status", "signing_date",
            "returned_archive", "returned_archive_date", "date_tripartite_agreement", "number_tripartite_agreement",
            "original_file_parsed_on", "sign", "is_obsolete_date"
        ]
