import json
import requests
from scripts.__init__ import *


def handle_message(logs: dict, message: str, total_lines: int) -> str:
    messages: list = []
    for queue, log_data in logs.items():
        count: int = log_data.get("count_message", 0)
        processed_table: str = log_data.get("processed_table")
        total_lines += count
        messages.append(
            f"Очередь: `{queue}`\n"
            f"Обработанная таблица: \n`{processed_table}`"
            f"\nКоличество сообщений: {count}\n"
        )

    if messages:
        message = "\n".join(messages)
        message += f"\n📊 *Общее количество строк: {total_lines}*"

    return message


def send_message():
    logger: get_logger = get_logger(os.path.basename(__file__).replace(".py", ""))
    logger.info("Send message to telegram")
    message: str = "Не было сообщений"
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as file:
            logs: dict = json.load(file)
        if isinstance(logs, dict):
            total_lines: int = 0
            message = handle_message(logs, message, total_lines)
    params: dict = {
        "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
        "text": f"\n{message}\n",
        "parse_mode": "MarkdownV2",
        "reply_to_message_id": get_my_env_var('MESSAGE_ID')
    }
    url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
    response = requests.get(url, params=params)
    response.raise_for_status()
    os.remove(LOG_FILE)
    return response


if __name__ == "__main__":
    send_message()
