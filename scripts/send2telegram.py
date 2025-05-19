import requests
from tinydb import TinyDB
from tinydb.table import Table
from scripts.__init__ import *


def handle_message(logs: dict, message: str, total_lines: int) -> str:
    messages: list = []
    for queue, log_data in logs.items():
        count: int = log_data.get("count_message", 0)
        processed_table: str = log_data.get("processed_table")
        total_lines += count
        messages.append(
            f"📥 Очередь: `{queue}`\n"
            f"📊 Обработанная таблица: \n`{processed_table}`\n"
            f"🔢 Количество сообщений: {count}\n"
        )

    if messages:
        message = "\n".join(messages)
        message += f"\n📈 Общее количество строк: {total_lines}"

    return message


def send_message():
    logger: get_logger = get_logger(str(os.path.basename(__file__).replace(".py", "")))
    logger.info("Send message to telegram")
    message: str = "Не было сообщений"
    if os.path.exists(LOG_FILE):
        db: TinyDB = TinyDB(LOG_FILE)
        stats_table: Table = db.table('stats')
        if logs := {
            item['queue_name']: item['data'] for item in stats_table.all()
        }:
            total_lines: int = 0
            message = handle_message(logs, message, total_lines)

    params: dict = {
        "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
        "text": f"Статистика обработки сообщений за день с RabbitMQ на сервере {get_my_env_var('HOST_HOSTNAME')}:\n"
                f"<blockquote expandable>{message}</blockquote>",
        "parse_mode": "HTML",
        "reply_to_message_id": get_my_env_var('MESSAGE_ID')
    }
    url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
    response = requests.get(url, params=params)
    response.raise_for_status()
    os.remove(LOG_FILE)
    return response


if __name__ == "__main__":
    send_message()
