import os
import copy
import json
import pytest
from typing import Union, Any
from pathlib import PosixPath
from scripts.receive import Receive
from unittest.mock import MagicMock
from datetime import date, datetime, time
from scripts.tables import RZHDOperationsReport, DataCoreClient

# Пример тела сообщения RabbitMQ
MESSAGE_BODY: dict = {
    "header": {
        "report": "ОтчетПоЖДПеревозкамМаркетингПоОперациям",
        "key_id": "24d198ac-1be2-11ef-809d-00505688b7b7",
        "parameters": {
            "period": {
                "dateStart": "2025-01-12T08:53:29",
                "dateEnd": "2025-01-12T08:53:31"
            }
        }
    },
    "data": [
        {
            "document_number": None,
            "etsng_code_ktk": None,
            "etsng_order_base_cargo": None,
            "etsng_operation_cargo": None,
            "is_border_crossing_point": False,
            "service": "Ж/д перевозка",
            "order_number": "ЖД-00014060",
            "destination_point_type": "Ж.Д. станция",
            "manager": "Жукова Ирина Сергеевна",
            "client_inn": "9715233916",
            "client_uid": "6e517f41-3dd7-11ea-800e-005056886559",
            "key_id": "24d198ac-1be2-11ef-809d-00505688b7b7",
            "client": "БЕГУ ООО ТК",
            "etsng_name": None,
            "container_size": "20",
            "departure_point_type": "Ж.Д. станция",
            "operation_month": 5,
            "operation_date": "2024-05-27T07:33:31",
            "operation_year": 2024,
            "container_type": "DC",
            "department": "ДКП Центр",
            "container_number": "TKRU3157739",
            "etsng_code": None,
            "destination_point": "БАТАРЕЙНАЯ (Ж.Д. станция)",
            "destination_station_code": "932601",
            "departure_station_code": "238601",
            "departure_point": "БЕЛЫЙ РАСТ (Ж.Д. станция)",
            "direction": "DOMESTIC",
            "type_of_relation": "Третьи стороны",
            "operation_name": "Организация ЖД перевозки (этап 1) (RN-022629)"
        },
        {
            "document_number": None,
            "etsng_code_ktk": None,
            "etsng_order_base_cargo": None,
            "etsng_operation_cargo": None,
            "is_border_crossing_point": False,
            "service": "Ж/д перевозка",
            "order_number": "ЖД-00014060",
            "destination_point_type": "Ж.Д. станция",
            "manager": "Жукова Ирина Сергеевна",
            "client_inn": "9715233916",
            "client_uid": "6e517f41-3dd7-11ea-800e-005056886559",
            "key_id": "24d198ac-1be2-11ef-809d-00505688b7b7",
            "client": "БЕГУ ООО ТК",
            "etsng_name": None,
            "container_size": "40",
            "departure_point_type": "Ж.Д. станция",
            "operation_month": 5,
            "operation_date": "2024-05-27T07:33:31",
            "operation_year": 2024,
            "container_type": "HC",
            "department": "ДКП Центр",
            "container_number": "AKMU4005754",
            "etsng_code": None,
            "destination_point": "БАТАРЕЙНАЯ (Ж.Д. станция)",
            "destination_station_code": "932601",
            "departure_station_code": "238601",
            "departure_point": "БЕЛЫЙ РАСТ (Ж.Д. станция)",
            "direction": "DOMESTIC",
            "type_of_relation": "Третьи стороны",
            "operation_name": "Организация ЖД перевозки (этап 1) (RN-022629)"
        }
    ]
}
LIST_COLUMNS: list = [
    'uuid', 'is_border_crossing_point', 'key_id', 'operation_name', 'operation_year',
    'document_number', 'direction', 'client_inn', 'service', 'container_type',
    'order_number', 'etsng_order_base_cargo', 'container_number', 'operation_date',
    'etsng_operation_cargo', 'manager', 'container_size', 'original_file_parsed_on',
    'original_operation_date_string', 'etsng_code_ktk', 'etsng_code', 'is_obsolete_date',
    'destination_station_code', 'departure_station_code', 'client_uid', 'departure_point',
    'department', 'departure_point_type', 'sign', 'destination_point', 'client', 'type_of_relation',
    'operation_month', 'destination_point_type', 'etsng_name'
]


@pytest.fixture
def mock_main(mocker: MagicMock) -> None:
    """
    Fixture for mocking all the main methods of class Receive and DataCoreClient.

    It patches the following methods:
    - Receive.main
    - Receive.send_stats
    - DataCoreClient.connect_to_db
    - DataCoreClient.handle_rows
    - DataCoreClient.insert_message
    - DataCoreClient.update_status
    - DataCoreClient.delete_old_deals
    - DataCoreClient.get_table_columns
    :param mocker: Mocker fixture
    :return:
    """
    mocker.patch("scripts.receive.Receive.main")
    mocker.patch("scripts.receive.Receive.send_stats", return_value=200)
    mocker.patch("scripts.receive.Receive.connect_to_db", return_value=MagicMock())
    mocker.patch("scripts.receive.DataCoreClient.handle_rows")
    mocker.patch("scripts.receive.DataCoreClient.insert_message")
    mocker.patch("scripts.receive.DataCoreClient.update_status")
    mocker.patch("scripts.receive.DataCoreClient.delete_old_deals")
    mocker.patch("scripts.receive.DataCoreClient.get_table_columns", return_value=LIST_COLUMNS)


@pytest.fixture
def receive_instance(mock_main: MagicMock) -> Receive:
    """
    Fixture for creating an instance of class Receive

    It creates an instance of class Receive and calls its main method.
    The main method is patched in fixture mock_main.
    :param mock_main: An instance of class Receive
    :return:
    """
    return Receive()


@pytest.fixture
def datacore_client_instance(receive_instance: Receive, mock_main: MagicMock) -> DataCoreClient:
    """
    Fixture for creating an instance of class DataCoreClient

    It creates an instance of class DataCoreClient.
    The connect_to_db method of DataCoreClient is patched in fixture mock_main.
    :param receive_instance: An instance of class Receive
    :param mock_main: An instance of class DataCoreClient
    :return:
    """
    return DataCoreClient(receive_instance)


@pytest.mark.parametrize("current_time, required_time, expected", [
    (time(hour=20, minute=1), time(hour=20, minute=0), True),
    (time(hour=19, minute=50), time(hour=20, minute=0), False),
])
def test_receive_check_and_update_log(
    receive_instance: Receive,
    current_time: time,
    required_time: time,
    expected: bool
) -> None:
    receive_instance.is_greater_time = expected
    is_updated: bool = receive_instance.check_and_update_log(
        current_time=current_time,
        required_time=required_time,
        time_sleep=1
    )
    assert is_updated == expected


@pytest.mark.parametrize("method, expected", [
    ("handle_incoming_json", (MESSAGE_BODY, RZHDOperationsReport, "24d198ac-1be2-11ef-809d-00505688b7b7")),
    ("parse_message", (MESSAGE_BODY, "ОтчетПоЖДПеревозкамМаркетингПоОперациям", "24d198ac-1be2-11ef-809d-00505688b7b7"))
])
def test_receive_methods(receive_instance: Receive, method: callable, expected: tuple) -> None:
    """
    Tests methods read_json and read_msg of class Receive.

    Method read_json reads a json file and returns the data from the file,
    an instance of RZHDOperationsReport class and a message id.

    Method read_msg reads a message from RabbitMQ and returns the data from
    the message, an instance of RZHDOperationsReport class and a message id.

    :param receive_instance: An instance of class Receive
    :param method: A string with the name of the method to test
    :param expected: A tuple with the expected result
    :return:
    """
    result = getattr(receive_instance, method)(MESSAGE_BODY)
    assert result[0] == expected[0]
    assert result[-1] == expected[-1]


@pytest.mark.parametrize("all_data, data_core, eng_table_name, key_deals", [
    (MESSAGE_BODY, RZHDOperationsReport, "rzhd_by_operations_report", "24d198ac-1be2-11ef-809d-00505688b7b7"),
])
def test_receive_parse_data(
    receive_instance: Receive,
    all_data: dict,
    data_core: Any,
    eng_table_name: str,
    key_deals: str
) -> None:
    """
    Tests the parse_data method of the Receive class.

    The parse_data method processes the given data and generates a file name
    based on the provided table name and key deals. This test verifies that
    the generated file name contains the expected English table name.

    :param receive_instance: An instance of the Receive class
    :param all_data: A dictionary containing the data to be processed
    :param data_core: An instance of a data core class or any relevant type
    :param eng_table_name: A string representing the expected table name in English
    :param key_deals: A string representing the key deals identifier
    :return:
    """
    data = copy.deepcopy(MESSAGE_BODY).get("data", [])
    file_name = receive_instance.process_data(all_data, data, data_core(receive_instance), eng_table_name, key_deals)

    assert eng_table_name in file_name


def test_receive_write_to_json(receive_instance: Receive, tmp_path: PosixPath) -> None:
    """
    Tests the write_to_json method of the Receive class.

    The write_to_json method writes the given data to a JSON file
    in the specified directory. This test verifies that a file is
    created and contains the expected data.

    :param receive_instance: An instance of the Receive class
    :param tmp_path: A temporary directory
    :return:
    """
    file_name: str = receive_instance.write_to_json(MESSAGE_BODY, "rzhd_test", dir_name=str(tmp_path))
    output_file: PosixPath = tmp_path / os.path.basename(file_name)
    assert output_file.exists()
    with open(output_file, "r", encoding="utf-8") as f:
        parsed_content = json.loads(f.read())

    assert isinstance(parsed_content, dict)
    assert parsed_content == MESSAGE_BODY


# Тесты для класса DataCoreClient и его наследников

@pytest.mark.parametrize("data, expected", [
    ({"KEY": "value"}, {"key": "value"}),
    ({"Key": "value"}, {"key": "value"}),
    ({"kEY": "value"}, {"key": "value"}),
])
def test_datacore_convert_to_lowercase(datacore_client_instance: DataCoreClient, data: dict, expected: dict) -> None:
    """
    Tests the convert_to_lowercase method of the DataCoreClient class.

    The convert_to_lowercase method converts the keys of the input dictionary
    to lowercase. This test verifies that the method correctly transforms
    keys to lowercase for various input cases.

    :param datacore_client_instance: An instance of the DataCoreClient class
    :param data: A dictionary with keys to be converted to lowercase
    :param expected: The expected dictionary with lowercase keys
    :return: None
    """
    assert datacore_client_instance.convert_to_lowercase(data) == expected


@pytest.mark.parametrize("data, columns, expected", [
    (
            {"price": "1 234,56", "price2": "1234.56"},
            {"float_columns": ["price", "price2"]},
            {"price": 1234.56, "price2": 1234.56}
    ),
    (
            {"year": "2023", "year2": "2 023"},
            {"int_columns": ["year", "year2"]},
            {"year": 2023, "year2": 2023}
    ),
    (
            {"date": "01.01.2023", "date2": "01.01.2023T12:53:58"},
            {"date_columns": ["date", "date2"]},
            {"date": date(2023, 1, 1), "date2": date(2023, 1, 1)}
    ),
    (
            {"date": "01.01.2023 12:53:58", "date2": "01.01.2023T12:53:58"},
            {"date_columns": ["date", "date2"], "is_datetime": True},
            {"date": datetime(2023, 1, 1, 12, 53, 58), "date2": datetime(2023, 1, 1, 12, 53, 58)}
    ),
    (
            {"status": "да", "status2": "Да", "status3": "нет", "status4": "Нет"},
            {"bool_columns": ["status", "status2", "status3", "status4"]},
            {"status": True, "status2": True, "status3": False, "status4": False}
    ),
])
def test_datacore_change_columns(
    datacore_client_instance: DataCoreClient,
    data: dict,
    columns: dict,
    expected: dict
) -> None:
    """
    Tests the change_columns method of the DataCoreClient class.

    The change_columns method processes the given data and transforms the values
    of the specified columns according to the given parameters. This test verifies
    that the method correctly transforms the values of the columns for various
    input cases.

    :param datacore_client_instance: An instance of the DataCoreClient class
    :param data: A dictionary with data to be processed
    :param columns: A dictionary with parameters for column transformation
    :param expected: The expected dictionary with transformed data
    :return: None
    """
    datacore_client_instance.change_columns(data, **columns)
    assert data == expected


@pytest.mark.parametrize("all_data, db_columns, rabbit_columns, expected", [
    ({"key": "value"}, [], [], []),
    ({"key": "value"}, ["col1"], [], ["col1"]),
    ({"key": "value"}, [], ["col2"], ["col2"]),
    ({"key": "value"}, ["col1"], ["col2"], ["col1", "col2"]),
])
def test_datacore_check_difference_columns(
    datacore_client_instance: DataCoreClient,
    all_data: dict,
    db_columns: list,
    rabbit_columns: list,
    expected: list
) -> None:
    """
    Tests the check_difference_columns method of the DataCoreClient class.

    The check_difference_columns method takes in all_data, db_columns, rabbit_columns
    and key_deals as parameters. It returns a list of column names which are not
    present in either the database or the RabbitMQ message. The test verifies that
    the method correctly identifies these columns by comparing the result with the
    expected output.

    :param datacore_client_instance: An instance of the DataCoreClient class
    :param all_data: A dictionary with data to be processed
    :param db_columns: A list of column names present in the database
    :param rabbit_columns: A list of column names present in the RabbitMQ message
    :param expected: The expected list of columns which are not present in either
                     the database or the RabbitMQ message
    :return: None
    """
    result = datacore_client_instance.check_difference_columns(
        all_data, "table", db_columns, rabbit_columns, "key"
    )
    assert result == expected


@pytest.mark.parametrize("date_string, is_datetime, expected", [
    ("01.01.2023T12:53:58", False, date(2023, 1, 1)),
    ("01.01.2023T12:53:58", True, datetime(2023, 1, 1, 12, 53, 58)),
    ("2023-01-01T12:53:58", False, date(2023, 1, 1)),
    ("2023-01-01", False, date(2023, 1, 1)),
    ("1912-12-31", False, date(1925, 1, 1)),
    ("test_string", False, "test_string")
])
def test_datacore_convert_format_date(
    datacore_client_instance: DataCoreClient,
    date_string: str,
    is_datetime: bool,
    expected: Union[date, datetime]
):
    """
    Tests the convert_format_date method of the DataCoreClient class.

    The convert_format_date method takes in a date string, a dictionary with data
    to be processed, a column name and a boolean indicating whether the date
    string represents a datetime or a date. It returns the parsed date or
    datetime. The test verifies that the method correctly parses the date string
    for various input cases.

    :param datacore_client_instance: An instance of the DataCoreClient class
    :param date_string: A string representing a date or datetime
    :param is_datetime: A boolean indicating whether the date string represents
                        a datetime or a date
    :param expected: The expected parsed date or datetime
    :return: None
    """
    data: dict = {"original_date_string": ''}
    datacore_client_instance.original_date_string = list(data.keys())[0]
    result: Union[date, datetime] = datacore_client_instance.convert_format_date(
        date_string, data, "test_column", is_datetime
    )
    assert result == expected
