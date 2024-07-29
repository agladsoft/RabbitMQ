import requests
from __init__ import *


def send_message():
    logger = get_logger(os.path.basename(__file__).replace(".py", ""))
    logger.info("Send message to telegram")
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as file:
            message = file.read()
    else:
        message = "Не было сообщений"
    params: dict = {
        "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
        "text": message,
        "reply_to_message_id": get_my_env_var('MESSAGE_ID')
    }
    url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
    # response = requests.get(url, params=params)
    # response.raise_for_status()
    # return response


if __name__ == "__main__":
    send_message()
