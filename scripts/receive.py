import fcntl
import asyncio
import sqlite3
import requests
import time as time_
from pathlib import Path
from pika.spec import Basic
from scripts.tables import *
from scripts.__init__ import *
from pika import BasicProperties
from datetime import datetime, time
from asyncio import AbstractEventLoop
from sqlite3 import Connection, Cursor
from scripts.rabbit_mq import RabbitMQ
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from typing import Tuple, Union, Optional, Any
from pika.adapters.blocking_connection import BlockingChannel


class Receive:
    def __init__(self, log_file: str = LOG_FILE):
        self.logger: logging.getLogger = get_logger(
            str(os.path.basename(__file__).replace(".py", "_") + str(datetime.now(tz=TZ).date()))
        )
        self.log_file: str = log_file
        self._init_db()
        self.rabbit_mq: RabbitMQ = RabbitMQ()
        self.client: Optional[Client] = None
        self.connect_to_db()
        self.count_message: int = 0
        self.is_greater_time: bool = False
        self.queue_name: Optional[str] = None
        self.table_name: Optional[str] = None
        self.message_errors: list = []
        self.queue_name_errors: list = []
        self.key_deals_buffer: list = []
        self.rows_buffer: list = []
        self.log_message_buffer: list = []
        self.delivery_tags: list = []

    def _init_db(self) -> None:
        """
        Initialize the SQLite database.

        This method creates a SQLite database file if it does not already exist.
        It creates a table 'stats' with columns for queue name, timestamp,
        count of messages, and processed table. The queue_name column is set as
        the primary key. This setup is used to store statistics related to message
        processing.

        :return: None
        """
        conn: Connection = sqlite3.connect(self.log_file)
        try:
            cursor: Cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    queue_name TEXT PRIMARY KEY,
                    timestamp TEXT,
                    count_message INTEGER,
                    processed_table TEXT
                )
            ''')
            conn.commit()
        finally:
            conn.close()

    def connect_to_db(self) -> Optional[Client]:
        """
        Connect to ClickHouse database.
        Establish a connection to the ClickHouse database. This method is called once when the script starts.
        :return: ClickHouse client.
        """
        try:
            self.client: Client = get_client(
                host=get_my_env_var('HOST'),
                database=get_my_env_var('DATABASE'),
                username=get_my_env_var('USERNAME_DB'),
                password=get_my_env_var('PASSWORD')
            )
        except Exception as ex_connect:
            self.logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            raise ConnectionError from ex_connect

    def load_stats(self) -> Optional[dict]:
        """
        Load statistics from SQLite database.
        :return: Loaded statistics as a dict.
        """
        conn: Connection = sqlite3.connect(self.log_file)
        try:
            cursor: Cursor = conn.cursor()
            cursor.execute('SELECT * FROM stats')
            rows: list = cursor.fetchall()
            return {
                row[0]: {
                    "timestamp": row[1],
                    "count_message": row[2],
                    "processed_table": row[3]
                }
                for row in rows
            }
        finally:
            conn.close()

    def save_stats(self, stats: dict) -> None:
        """
        Save statistics to SQLite database.
        :param stats: Statistics for save.
        :return: None
        """
        conn: Connection = sqlite3.connect(self.log_file)
        try:
            cursor: Cursor = conn.cursor()
            for queue_name, data in stats.items():
                cursor.execute('''
                    INSERT OR REPLACE INTO stats (queue_name, timestamp, count_message, processed_table)
                    VALUES (?, ?, ?, ?)
                ''', (
                    queue_name,
                    data["timestamp"],
                    data["count_message"],
                    data["processed_table"]
                ))
            conn.commit()
        finally:
            conn.close()

    def update_stats(self) -> None:
        """
        Update statistics in SQLite database.

        Load statistics from SQLite database. If self.queue_name not in statistics,
        create new statistics for this queue. Update count messages and processed table for this queue.
        Save statistics to SQLite database.
        :return: None
        """
        stats: dict = self.load_stats()
        today: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if self.queue_name not in stats:
            stats[self.queue_name] = {
                "timestamp": today,
                "count_message": 0,
                "processed_table": None
            }

        stats[self.queue_name]["count_message"] += self.count_message
        stats[self.queue_name]["processed_table"] = self.table_name
        stats[self.queue_name]["timestamp"] = today

        self.save_stats(stats)

    def _check_and_update_log(
        self,
        current_time: time = datetime.now(tz=TZ).time().replace(second=0, microsecond=0),
        required_time: time = time(hour=19, minute=58),
        time_sleep: int = 300
    ) -> bool:
        """
        Check time and update log if necessary.

        If current time greater than required time and self.is_greater_time is True,
        method create log file for writing count messages, update statistics in log file,
        reset count of messages and table name, and set self.is_greater_time to False.
        Method also sleep for time_sleep seconds.

        If current time lesser than required time and self.is_greater_time is False,
        method set self.is_greater_time to True.

        :param current_time: Current time for compare with required time.
        :param required_time: Required time for compare with current time.
        :param time_sleep: Time for sleep in seconds.
        :return: True if log was updated, False otherwise.
        """
        if current_time >= required_time and self.is_greater_time:
            self.logger.info("Created log file for writing count messages")
            self.update_stats()
            self.count_message = 0
            self.table_name = None
            self.is_greater_time = False
            time_.sleep(time_sleep)
            return True
        elif current_time <= required_time and not self.is_greater_time:
            self.logger.info("current_time lesser required_time and self.is_greater_time = True")
            self.is_greater_time = True
        return False

    def _send_with_retries(self, message: str) -> Optional[requests.Response]:
        """
        Send a message to the Telegram bot with exponential backoff.

        Send a message to the Telegram bot with exponential backoff in case of errors.
        If the message couldn't be sent after 3 attempts, method returns None.

        :param message: Message to be sent.
        :return: Response from the Telegram API, or None if the message couldn't be sent.
        """
        params: dict = {
            "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
            "text": f"\n{message}\n",
            "parse_mode": "MarkdownV2",
            "reply_to_message_id": get_my_env_var('MESSAGE_ID')
        }
        url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"

        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=120)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                self.logger.warning(f"Sending error ({attempt + 1}/3): {e}")
                time_.sleep(30 * (2 ** attempt))  # Exponential backoff: 30s, 60s, 120s

        self.logger.error("Couldn't send a message after 3 attempts")
        return None

    def send_stats(self) -> Optional[requests.Response]:
        """
        Send statistics to Telegram.

        Sends a message to a specified Telegram chat with statistics about the processed queue.
        The message includes the queue name, processed table, error count, and error details.
        The method attempts to send the message up to three times, with a 30-second delay
        between attempts in case of failure.  If errors occurred during processing, they are
        cleared after a successful send. If sending fails after three attempts, an error is logged.

        :return: The response from the Telegram API if the message was sent successfully,
                 None otherwise, or if no errors occurred or no messages were processed.
        """
        if self.count_message == 0:
            return None

        message: str = (
            f"\n"
            f"📥 Очередь: `{self.queue_name}` пустая\n"
            f"📊 Обработанная таблица: `{self.table_name}`\n"
            f"🔢 Количество сообщений: {self.count_message}\n"
            f"🚨 Количество ошибок: {len(self.message_errors)}\n"
            f"⚠️ Ошибки: `{self.message_errors}`"
        )[:4090]
        self.logger.info(message)
        if not self.message_errors:
            self.update_stats()
            return None

        self.message_errors = []
        return self._send_with_retries(message)

    def callback(
        self,
        ch: Union[BlockingChannel, str],
        method: Union[Basic.Deliver, str],
        properties: Union[BasicProperties, str],
        body: Union[bytes, str],
        message_count: int
    ) -> None:
        """
        Callback function to process messages from the queue.

        This function logs the start of the callback, increments the message count,
        and processes the message data. If the data contains a valid core, it will handle
        the rows; otherwise, it logs an error and attempts to insert the message into the
        data core client. Finally, it logs the completion of the callback.

        :param ch: The channel from which the message is received.
        :param method: The delivery method of the message.
        :param properties: The properties of the message.
        :param body: The body of the message, which is expected to be in bytes or string format.
        :param message_count: The count of messages in the queue.
        :return:
        """
        self._check_and_update_log()
        self.count_message += 1
        self.logger.info(
            f"Callback start for ch={ch}, method={method}, properties={properties}, body_message called. "
            f"Count messages is {self.count_message}"
        )
        all_data, data, data_core, key_deals = self.handle_incoming_json(body, message_count)
        if data_core:
            is_success_inserted: Optional[bool] = None if key_deals is None else True
            data_core.handle_rows(all_data, data, key_deals, message_count, is_success_inserted)
        else:
            data_core_client = DataCoreClient
            data_core_client.table = all_data.get("header", {}).get("report")
            data_core_client(self).insert_message(all_data, key_deals, message_count, is_success_inserted=False)
            raise AssertionError(f"Not found table name in dictionary. Russian table is {self.table_name}")
        self.logger.info("Callback exit. The data from the queue was processed by the script")

    def process_data(
        self,
        all_data: dict,
        data: list,
        data_core: Any,
        eng_table_name: str,
        key_deals: str,
        message_count: int
    ) -> None:
        """
        Processes the given data, converting it to the required format and structure.

        This method reads the provided JSON data, modifies it according to the specifications
        of the data core, and generates a file name based on the English table name and
        the current timestamp. It logs the process and handles any exceptions that occur
        during data conversion. If the data contains discrepancies in column names, it raises
        an assertion error.

        :param all_data: A dictionary containing the entire dataset to be processed.
        :param data: A list of data rows to be processed.
        :param data_core: An instance of a data core class that provides methods for data manipulation.
        :param eng_table_name: A string representing the English name of the table.
        :param key_deals: A string identifier for key deals.
        :param message_count: The count of messages in the queue.
        :return: A string representing the generated file name.
        :raises AssertionError: If there are errors in data conversion or column name discrepancies.
        """
        file_name: str = f"{eng_table_name}_{datetime.now(tz=TZ)}.json"
        self.logger.info(f'Starting read json. Length of json: {len(data)}. Table: {eng_table_name}')
        list_columns_db = list(set(data_core.get_table_columns()) - set(data_core.removed_columns_db))
        original_date_string: str = data_core.original_date_string
        lowercase_data: bool = isinstance(data_core, FreightRates)
        try:
            for i in range(len(data)):
                if lowercase_data:
                    data[i] = data_core.convert_to_lowercase(data[i])
                data_core.add_new_columns(data[i], file_name, original_date_string)
                data_core.change_columns(data=data[i])
                if original_date_string:
                    data[i][original_date_string] = data[i][original_date_string].strip() or None
        except Exception as ex:
            self.logger.error(f"Error converting data types. Table: {eng_table_name}. Exception: {ex}")
            data_core.insert_message(all_data, key_deals, message_count, is_success_inserted=False)
            raise AssertionError("Stop consuming because receive an error where converting data types") from ex
        if data and data_core.check_difference_columns(
            all_data, list_columns_db, list(data[0].keys()), key_deals, message_count
        ):
            raise AssertionError("Stop consuming because columns is different")

    @staticmethod
    def _parse_message(msg: Union[bytes, str, dict]) -> Tuple[dict, str, str, bool]:
        """
        Decodes and parses a message to extract relevant information.

        This method takes a message in the form of bytes, string, or dictionary.
        It decodes the message if it is in bytes format, and then parses it to
        extract the entire data, Russian table name, and key deals identifier.

        :param msg: The message to be processed, which can be bytes, string, or dictionary.
        :return: A tuple containing the entire data as a dictionary, the Russian table name as a string,
                 and the key deals identifier as a string.
        """
        msg: str = msg.decode('utf-8-sig') if isinstance(msg, (bytes, bytearray)) else msg
        all_data: dict = json.loads(msg) if isinstance(msg, str) else msg
        rus_table_name: str = all_data.get("header", {}).get("report")
        key_deals: str = all_data.get("header", {}).get("key_id")
        is_truncate: bool = all_data.get("header", {}).get("is_truncate")
        return all_data, rus_table_name, key_deals, is_truncate

    def handle_incoming_json(
        self,
        msg: Union[bytes, str, dict],
        message_count: int = 0
    ) -> Tuple[dict, list, Any, Optional[str]]:
        """
        Handles an incoming JSON message.

        This method takes a message in the form of bytes, string, or dictionary.
        It decodes the message if it is in bytes format, and then parses it to
        extract the entire data, Russian table name, and key deals identifier.

        It then uses the Russian table name to look up the corresponding English
        table name in the TABLE_NAMES dictionary. If the English table name is found,
        it creates an instance of the corresponding data core class and uses it to
        process the data. The processed data is then saved to a JSON file, and the
        name of the file is returned along with the other data.

        If the English table name is not found, it logs an error and writes the
        original data to a file in the errors directory.

        :param msg: The message to be processed, which can be bytes, string, or dictionary.
        :param message_count: The count of messages in the queue.
        :return: A tuple containing the entire data as a dictionary, the Russian table name as a string,
                 the name of the JSON file as a string (or None if the English table name is not found),
                 the data core instance (or None if the English table name is not found), and the key deals
                 identifier as a string.
        """
        all_data, rus_table_name, key_deals, is_truncate = self._parse_message(msg)
        eng_table_name: str = TABLE_NAMES.get(rus_table_name)
        self.table_name = eng_table_name
        data: list = list(all_data.get("data", []))
        data_core: Any = CLASS_NAMES_AND_TABLES.get(eng_table_name)
        if data_core:
            data_core.table = eng_table_name
            data_core: Any = data_core(self)
            if is_truncate and not data:
                self.logger.warning(f"Data needs to be truncated. Table is {eng_table_name}")
                data_core.delete_old_deals(cond="key_id IS NOT NULL")
                return all_data, data, data_core, None
            self.process_data(all_data, data, data_core, eng_table_name, key_deals, message_count)
        else:
            self.logger.error(f"Not found table name in dictionary. Russian table is {rus_table_name}")
            self.table_name = rus_table_name
        return all_data, data, data_core, key_deals

    def write_to_json(
        self,
        msg: dict,
        eng_table_name: str,
        dir_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/json"
    ) -> str:
        """
        Writes a JSON message to a file.

        This method takes a dictionary message, an English table name, and an optional
        directory name as parameters. It saves the message to a JSON file with the
        given English table name and current timestamp as the file name. If the
        directory does not exist, it creates it. The method returns the name of the
        file.

        :param msg: The dictionary message to be saved to a file.
        :param eng_table_name: The English table name to be used in the file name.
        :param dir_name: The directory name where the file should be saved.
                         Defaults to the `XL_IDP_PATH_RABBITMQ` environment variable with `/json` appended.
        :return: The name of the file where the message was saved.
        """
        self.logger.info(f"Saving data to file {datetime.now(tz=TZ)}_{eng_table_name}.json")
        file_name: str = f"{dir_name}/{datetime.now(tz=TZ)}_{eng_table_name}.json"
        fle: Path = Path(file_name)
        if not os.path.exists(fle.parent):  # Проверяем, существует ли директория
            os.makedirs(fle.parent)  # Создаем директорию, если не существует

        with open(file_name, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            json.dump(msg, f, indent=4, ensure_ascii=False, default=serialize_datetime)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        return file_name

    def process_queue(self, queue_name: str) -> None:
        """
        Processes a queue.

        This method processes a queue by consuming messages from it and executing
        the callback function on each message. It also handles errors by logging
        them and re-queueing the message.

        :param queue_name: The name of the queue to be processed.
        :return:
        """
        self.queue_name: str = queue_name
        self.count_message: int = 0

        while True:
            method_frame, header_frame, body = self.rabbit_mq.get(queue_name)

            if not method_frame or method_frame.NAME == 'Basic.GetEmpty':
                self.send_stats()
                break

            self.logger.info(f"Got message with queue_name: {queue_name}")
            queue_info = self.rabbit_mq.channel.queue_declare(queue=queue_name, passive=True)
            message_count = queue_info.method.message_count  # Количество сообщений в очереди
            try:
                self.delivery_tags.append(method_frame.delivery_tag)
                self.callback(self.rabbit_mq.channel, method_frame, header_frame, body, message_count)
            except Exception as e:
                self.logger.error(f"Ошибка обработки: {e}")
                self.message_errors.append(self._parse_message(body)[2])
                self.rabbit_mq.channel.basic_nack(delivery_tag=self.delivery_tags[-1], multiple=True)
                self.queue_name_errors.append(queue_name)
                self.key_deals_buffer: list = []
                self.rows_buffer: list = []
                self.log_message_buffer: list = []
                self.delivery_tags: list = []
                self.send_stats()
                break

    async def async_main(self):
        """
        Asynchronously processes multiple RabbitMQ queues in parallel.

        This method sets up a loop to continuously process messages from multiple
        RabbitMQ queues. It uses an asyncio semaphore to limit the number of
        concurrent tasks to 10. For each queue, it creates and binds the queue
        once and then continually processes messages from it, unless the queue
        is in the error list. The method sleeps for a specified delay between
        processing cycles.

        A separate limited_process coroutine is used to handle the processing
        of each queue, allowing for parallel execution of up to 10 queues.

        :return: None
        """
        loop: AbstractEventLoop = asyncio.get_running_loop()
        delay: int = 60
        semaphore: asyncio.Semaphore = asyncio.Semaphore(10)  # максимум 10 параллельных задач

        async def limited_process(queue_name_):
            async with semaphore:
                receive_instance: Receive = Receive()
                receive_instance.queue_name_errors = self.queue_name_errors
                await loop.run_in_executor(None, receive_instance.process_queue, queue_name_)

        # 1. Создаём и привязываем очереди один раз
        ip_server: str = get_my_env_var('HOST_HOSTNAME')
        for queue_name, routing_key in QUEUES_AND_ROUTING_KEYS.items():
            if SERVER_AND_SUFFIX_QUEUE.get(queue_name.split("_")[-1]) != ip_server:
                raise "Queues don't match servers"
            self.rabbit_mq.declare_and_bind_queue(queue_name, routing_key)
        while True:
            tasks: list = [
                limited_process(queue_name)
                for queue_name in QUEUES_AND_ROUTING_KEYS.keys()
                if queue_name not in self.queue_name_errors
            ]
            if tasks:
                await asyncio.gather(*tasks)
            await asyncio.sleep(delay)


CLASSES: list = [
    # Данные по DC
    AccountingDocumentsRequests,
    Accounts,
    AutoVisits,
    AutoPickupGeneralReport,
    CompletedRepackagesReport,
    Consignments,
    CounterParties,
    DailySummary,
    DataCoreFreight,
    DevelopmentCounterpartyDepartment,
    ExportBookings,
    FreightRates,
    ImportBookings,
    MarginalityOrdersActDate,
    NaturalIndicatorsContractsSegments,
    NaturalIndicatorsTransactionFactDate,
    NaturalIndicatorsRailwayReceptionDispatch,
    OrdersMarginalityReport,
    OrdersReport,
    ReferenceLocations,
    RusconProducts,
    RZHDOperationsReport,
    SalesPlan,
    TerminalsCapacity,
    TransportUnits,
    RegisterOrders,
    RegisterOrdersContainer,
    RegisterOrdersSegment,
    RegisterOrdersTransportUnits,
    RegisterOrdersFinancialSpending,
    RegisterOrdersMarginalIncome,

    # Данные по DO
    ManagerEvaluation,
    ReferenceCounterparties,
    ReferenceContracts,
    Staff
]
CLASS_NAMES_AND_TABLES: dict = dict(zip(list(TABLE_NAMES.values()), CLASSES))

if __name__ == '__main__':
    # Для запуска асинхронного main
    asyncio.run(Receive().async_main())
