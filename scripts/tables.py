import re
import pytz
import json
import contextlib
from typing import TYPE_CHECKING
from typing import Union, Optional
from scripts.__init__ import LOG_TABLE
from datetime import datetime, date, timedelta

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
        return self.receive.client.database

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
        key_deals: str
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
        :return: A list of columns which are not present in either the database or the RabbitMQ message.
        """
        diff_db: list = list(set(list_columns_db) - set(list_columns_rabbit))
        diff_rabbit: list = list(set(list_columns_rabbit) - set(list_columns_db))
        if diff_db or diff_rabbit:
            self.receive.logger.error(
                f"The difference in columns {diff_db} from the database. "
                f"The difference in columns {diff_rabbit} from the rabbit"
            )
            self.insert_message(all_data, key_deals, is_success_inserted=False)
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

    def insert_message(self, all_data: dict, key_deals: str, is_success_inserted: bool):
        """
        Inserts a message into the log table with metadata and the provided data.

        This method truncates the 'data' field in 'all_data' to a maximum of 100 entries,
        constructs a row with various metadata including the database, table, queue name,
        key deals, and the success status, and then inserts this row into the specified
        log table in 'DataCore' database.

        :param all_data: A dictionary containing the data to be logged.
        :param key_deals: A string identifier for the key deals.
        :param is_success_inserted: A boolean flag indicating if the data insertion was successful.
        :return:
        """
        len_data: int = 100
        all_data["data"] = all_data["data"][:len_data] if len(all_data["data"]) >= len_data else all_data["data"]
        rows = [[
            self.database,
            self.table,
            self.receive.queue_name,
            key_deals,
            datetime.now(tz=TZ) + timedelta(hours=3),
            is_success_inserted,
            json.dumps(all_data, default=serialize_datetime, ensure_ascii=False, indent=2)
        ]]
        columns = ["database", "table", "queue", "key_id", "datetime", "is_success", "message"]
        self.receive.client.insert(
            table=LOG_TABLE,
            database="DataCore",
            data=rows,
            column_names=columns,
            settings={"async_insert": 1, "wait_for_async_insert": 1}
        )

    def handle_rows(self, all_data, data: list, key_deals: str) -> None:
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
        :return: None
        """
        try:
            self.update_status(key_deals)
            rows = [list(row.values()) for row in data] if data else [[]]
            columns = list(data[0]) if data else []
            if rows and columns:
                self.receive.client.insert(
                    table=self.table,
                    database=self.database,
                    data=rows,
                    column_names=columns,
                    settings={"async_insert": 1, "wait_for_async_insert": 1}
                )
                self.receive.logger.info("The data has been uploaded to the database")
            self.insert_message(all_data, key_deals, is_success_inserted=True)
        except Exception as ex:
            self.receive.logger.error(f"Exception is {ex}. Type of ex is {type(ex)}")
            self.insert_message(all_data, key_deals, is_success_inserted=False)
            raise ConnectionError(ex) from ex

    def update_status(self, key_deals: str) -> None:
        """
        Updates the status of entries in the database where the sum of the 'sign' column is greater than zero.

        This method constructs a query to select UUIDs that have a sum of 'sign' greater than zero
        for a given key deal. For these entries, it updates the 'sign' column to -1 and re-inserts
        the modified rows into the database. The process is logged upon completion.

        :param key_deals: A string identifier for key deals which is used to filter the database entries.
        :return:
        """
        query: str = (
            f"SELECT * FROM {self.database}.{self.table} WHERE uuid IN ("
            f"SELECT uuid FROM {self.database}.{self.table} WHERE {self.deal} = '{key_deals}' "
            f"GROUP BY uuid HAVING SUM(sign) > 0"
            f")"
        )
        selected_query = self.receive.client.query(query)
        if rows_query := selected_query.result_rows:
            list_rows: list = [
                {column: -1 if column == 'sign' else value
                 for value, column in zip(row, selected_query.column_names)}
                for row in rows_query
            ]
            rows: list = [list(row.values()) for row in list_rows]
            columns: list = list(list_rows[0].keys())
            self.receive.client.insert(
                table=self.table,
                database=self.database,
                data=rows,
                column_names=columns,
                settings={"async_insert": 1, "wait_for_async_insert": 1}
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

    def __exit__(self, exception_type, exception_val, trace):
        try:
            self.receive.client.close()
            self.receive.logger.info("Success disconnect clickhouse")
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


class CounterParties(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)


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


class TransportUnits(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)


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


class ImportBookings(DataCoreClient):
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
            date_columns=['etd', 'eta', 'booking_date', 'sob'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )


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


class DailySummary(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    @property
    def original_date_string(self):
        return "original_motion_date_string"

    def change_columns(self, *args, **kwargs) -> None:
        super().change_columns(
            data=kwargs.get('data'),
            float_columns=['tonnage', 'cargo_weight'],
            int_columns=['tare_weight', 'container_size'],
            date_columns=['motion_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=True
        )


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
            date_columns=['operation_date'],
            bool_columns=kwargs.get('bool_columns', []),
            is_datetime=kwargs.get('is_datetime', False)
        )


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
