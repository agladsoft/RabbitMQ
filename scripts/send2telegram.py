import sqlite3
import requests
from scripts.__init__ import *
from notifiers import get_notifier
from sqlite3 import Connection, Cursor


def send_email_notifiers(message: str, subject: str = "Уведомление от системы экспорта"):
    """
    Отправка email через Mail.ru
    """
    try:
        email = get_notifier('email')
        email.notify(
            to=get_my_env_var('RECIPIENT_EMAIL'),
            subject=subject,
            message=message,
            from_=get_my_env_var('EMAIL_USER'),
            host='smtp.mail.ru',
            port=587,
            username=get_my_env_var('EMAIL_USER'),
            password=get_my_env_var('EMAIL_PASSWORD'),
            tls=True
        )
        print(f"Email успешно отправлен на {get_my_env_var('RECIPIENT_EMAIL')}")
    except Exception as e:
        print(f"Ошибка при отправке email: {e}")

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
        conn: Connection = sqlite3.connect(LOG_FILE)
        try:
            cursor: Cursor = conn.cursor()
            cursor.execute('SELECT * FROM stats')
            if rows := cursor.fetchall():
                logs: dict = {
                    row[0]: {
                        "timestamp": row[1],
                        "count_message": row[2],
                        "processed_table": row[3]
                    }
                    for row in rows
                }
                total_lines: int = 0
                message = handle_message(logs, message, total_lines)

                # Очищаем таблицу после отправки сообщения
                cursor.execute('DELETE FROM stats')
                conn.commit()
        finally:
            conn.close()

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
    send_email_notifiers(params.get('text'))
    return response


if __name__ == "__main__":
    send_message()
