import json
import requests
from __init__ import *


def send_message():
    logger = get_logger(os.path.basename(__file__).replace(".py", ""))
    logger.info("Send message to telegram")
    message = "Не было сообщений"
    total_lines = 0

    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as file:
            logs = json.load(file)

        if isinstance(logs, dict):
            messages = []
            for queue, log_data in logs.items():
                count = log_data.get("count_message", 0)
                processed_table = log_data.get("processed_table")
                total_lines += count
                messages.append(
                    f"Очередь: `{queue}`\n"
                    f"Обработанная таблица: \n`{processed_table}`"
                    f"\n`Количество` сообщений: {count}\n"
                )

            if messages:
                message = "\n".join(messages)
                message += f"\n📊 *Общее количество строк: {total_lines}*"
    params: dict = {
        "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
        "text": f"\n{message}\n",
        "parse_mode": "MarkdownV2",
        "reply_to_message_id": get_my_env_var('MESSAGE_ID')
    }
    url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


if __name__ == "__main__":
    send_message()
