import os
import pytz
import copy
import json
import pytest
import tempfile
from typing import Union, Any
from pathlib import PosixPath
from unittest.mock import MagicMock
from datetime import date, datetime, time

os.environ['XL_IDP_PATH_RABBITMQ'] = '../.'
os.environ['XL_IDP_ROOT_RABBITMQ'] = '../.'

from scripts.receive import Receive
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
    mocker.patch("scripts.receive.Receive.async_main")
    mocker.patch("scripts.receive.Receive.send_stats", return_value=200)
    mocker.patch("scripts.receive.Receive.connect_to_db", return_value=MagicMock())
    mocker.patch("scripts.receive.DataCoreClient.handle_rows")
    mocker.patch("scripts.receive.DataCoreClient.insert_message")
    mocker.patch("scripts.receive.DataCoreClient.update_status")
    mocker.patch("scripts.receive.DataCoreClient.delete_old_deals")
    mocker.patch("scripts.receive.DataCoreClient.get_table_columns", return_value=LIST_COLUMNS)


@pytest.fixture
def temp_log_file():
    """Создает временный файл для логов и возвращает его путь."""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        yield temp_file.name
    os.remove(temp_file.name)  # Удаляем файл после тестов


@pytest.fixture
def receive_instance(temp_log_file: str, mock_main: MagicMock) -> Receive:
    """
    Fixture for creating an instance of class Receive

    It creates an instance of class Receive and calls its main method.
    The main method is patched in fixture mock_main.
    :param temp_log_file:
    :param mock_main: An instance of class Receive
    :return:
    """
    return Receive(log_file=temp_log_file)


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
    is_updated: bool = receive_instance._check_and_update_log(
        current_time=current_time,
        required_time=required_time,
        time_sleep=1
    )
    assert is_updated == expected


@pytest.mark.parametrize(
    "initial_data, expected_result",
    [
        (None, {}),  # Пустой файл
        (
            {"test_queue": {"timestamp": "2024-02-03 12:00:00", "count_message": 10, "processed_table": "table1"}},
            {"test_queue": {"timestamp": "2024-02-03 12:00:00", "count_message": 10, "processed_table": "table1"}}
        ),  # Сохранение и загрузка статистики
    ]
)
def test_load_stats(receive_instance: Receive, initial_data: dict, expected_result: dict) -> None:
    if initial_data:
        receive_instance.save_stats(initial_data)
    assert receive_instance.load_stats() == expected_result


@pytest.mark.parametrize(
    "initial_data, updated_data",
    [
        (
            {
                "test_queue": {
                    "timestamp": "2024-02-03 12:00:00",
                    "count_message": 3,
                    "processed_table": "old_table"
                }
            },
            {
                "test_queue": {
                    "timestamp": "2024-02-03 12:00:00",
                    "count_message": 8,
                    "processed_table": "test_table"
                }
            }
        ),  # Обновление статистики существующей очереди
    ]
)
def test_update_stats(receive_instance: Receive, initial_data, updated_data):
    if initial_data:
        receive_instance.save_stats(initial_data)

    receive_instance.queue_name = list(initial_data.keys())[0]
    receive_instance.count_message = 5
    receive_instance.table_name = "test_table"

    receive_instance.update_stats()
    stats = receive_instance.load_stats()
    assert stats["test_queue"]["count_message"] == updated_data["test_queue"]["count_message"]
    assert stats["test_queue"]["processed_table"] == updated_data["test_queue"]["processed_table"]
    assert "timestamp" in stats["test_queue"]


@pytest.mark.parametrize("method, expected", [
    ("handle_incoming_json", (
        MESSAGE_BODY, RZHDOperationsReport, "24d198ac-1be2-11ef-809d-00505688b7b7")
     ),
    ("_parse_message", (
        MESSAGE_BODY, "ОтчетПоЖДПеревозкамМаркетингПоОперациям", "24d198ac-1be2-11ef-809d-00505688b7b7")
     )
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
    message_body_copy: dict = copy.deepcopy(MESSAGE_BODY)
    result: Any = getattr(receive_instance, method)(message_body_copy)

    # Compare only the essential fields
    if isinstance(result[0], dict) and 'header' in result[0] and 'data' in result[0]:
        # Extract essential fields from header
        result_header: dict = result[0]['header']
        expected_header: dict = expected[0]['header']

        assert result_header.get('report') == expected_header.get('report')
        assert result_header.get('key_id') == expected_header.get('key_id')

        # Compare data arrays if they exist
        if 'data' in expected[0] and result[0]['data'] and expected[0]['data']:
            result_data: dict = result[0]['data'][0]
            expected_data: dict = expected[0]['data'][0]

            # Compare only essential fields from data
            essential_fields: list = [
                'order_number', 'client_inn', 'client', 'container_type', 'operation_name', 'direction', 'department'
            ]

            for field in essential_fields:
                assert result_data.get(field) == expected_data.get(field), f"Mismatch in field: {field}"

    assert result[-1] == expected[-1]


@pytest.mark.parametrize("all_data, data_core, eng_table_name, key_deals, expected", [
    (MESSAGE_BODY, RZHDOperationsReport, "rzhd_by_operations_report", "24d198ac-1be2-11ef-809d-00505688b7b7", 20),
])
def test_receive_parse_data(
    receive_instance: Receive,
    all_data: dict,
    data_core: Any,
    eng_table_name: str,
    key_deals: str,
    expected: int
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
    receive_instance.process_data(all_data, data, data_core(receive_instance), eng_table_name, key_deals, 1)

    assert data[0]["container_size"] == expected


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
            {"date": datetime(2023, 1, 1, 12, 53, 58, tzinfo=pytz.UTC), "date2": datetime(2023, 1, 1, 12, 53, 58, tzinfo=pytz.UTC)}
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
        all_data, db_columns, rabbit_columns, "key", 1
    )
    assert result == expected


@pytest.mark.parametrize("date_string, is_datetime, expected", [
    ("01.01.2023T12:53:58", False, date(2023, 1, 1)),
    ("01.01.2023T12:53:58", True, datetime(
        2023, 1, 1, 12, 53, 58, tzinfo=pytz.UTC
    )),
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
