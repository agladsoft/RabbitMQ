import copy
import os
import json
import pytest
from typing import Union, Any
from pathlib import PosixPath
from unittest.mock import MagicMock
from datetime import date, datetime, time
from scripts.receive import Receive, RZHDOperationsReport, DataCoreClient

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
def mock_main(mocker):
    mocker.patch("scripts.receive.Receive.main")
    mocker.patch("scripts.receive.Receive.check_queue_empty", return_value=200)
    mocker.patch("scripts.receive.DataCoreClient.connect_to_db", return_value=MagicMock())
    mocker.patch("scripts.receive.DataCoreClient.handle_rows")
    mocker.patch("scripts.receive.DataCoreClient.insert_message")
    mocker.patch("scripts.receive.DataCoreClient.update_status")
    mocker.patch("scripts.receive.DataCoreClient.delete_old_deals")
    mocker.patch("scripts.receive.DataCoreClient.get_table_columns", return_value=LIST_COLUMNS)


@pytest.fixture
def receive_instance(mock_main):
    obj = Receive()
    obj.main()
    return obj


@pytest.fixture
def datacore_client_instance(mock_main):
    return DataCoreClient()


@pytest.mark.parametrize("current_time, required_time, expected", [
    (time(hour=20, minute=1), time(hour=20, minute=0), True),
    (time(hour=19, minute=50), time(hour=20, minute=0), False),
])
def test_receive_check_and_update_log(receive_instance, current_time: time, required_time: time, expected: bool):
    receive_instance.is_greater_time = expected
    is_updated: bool = receive_instance.check_and_update_log(
        current_time=current_time,
        required_time=required_time,
        time_sleep=1
    )
    assert is_updated == expected


@pytest.mark.parametrize("method, expected", [
    ("read_json", (MESSAGE_BODY, RZHDOperationsReport, "24d198ac-1be2-11ef-809d-00505688b7b7")),
    ("read_msg", (MESSAGE_BODY, "ОтчетПоЖДПеревозкамМаркетингПоОперациям", "24d198ac-1be2-11ef-809d-00505688b7b7"))
])
def test_receive_methods(receive_instance, method, expected):
    result = getattr(receive_instance, method)(MESSAGE_BODY)
    assert result[0] == expected[0]
    assert result[-1] == expected[-1]


@pytest.mark.parametrize("all_data, data_core, eng_table_name, key_deals", [
    (MESSAGE_BODY, RZHDOperationsReport, "rzhd_by_operations_report", "24d198ac-1be2-11ef-809d-00505688b7b7"),
])
def test_receive_parse_data(
        receive_instance,
        all_data: dict,
        data_core: Any,
        eng_table_name: str,
        key_deals: str
):
    data = copy.deepcopy(MESSAGE_BODY).get("data", [])
    file_name = receive_instance.parse_data(all_data, data, data_core(), eng_table_name, key_deals)

    assert eng_table_name in file_name


def test_receive_write_to_json(receive_instance: Receive, tmp_path: PosixPath) -> None:
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
def test_datacore_convert_to_lowercase(datacore_client_instance, data, expected):
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
def test_datacore_change_columns(datacore_client_instance, data, columns, expected):
    datacore_client_instance.change_columns(data, **columns)
    assert data == expected


@pytest.mark.parametrize("all_data, db_columns, rabbit_columns, expected", [
    ({"key": "value"}, [], [], []),
    ({"key": "value"}, ["col1"], [], ["col1"]),
    ({"key": "value"}, [], ["col2"], ["col2"]),
    ({"key": "value"}, ["col1"], ["col2"], ["col1", "col2"]),
])
def test_datacore_check_difference_columns(datacore_client_instance, all_data, db_columns, rabbit_columns, expected):
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
    data: dict = {"original_date_string": ''}
    datacore_client_instance.original_date_string = list(data.keys())[0]
    result: Union[date, datetime] = datacore_client_instance.convert_format_date(
        date_string, data, "test_column", is_datetime
    )
    assert result == expected
