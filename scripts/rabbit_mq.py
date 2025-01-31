import pika
from scripts.__init__ import get_my_env_var


class RabbitMQ:
    def __init__(self):
        self.user = get_my_env_var('RABBITMQ_USER')
        self.password = get_my_env_var('RABBITMQ_PASSWORD')
        self.host = get_my_env_var('RABBITMQ_HOST')
        self.port = int(get_my_env_var('RABBITMQ_PORT'))
        self.exchange_name = get_my_env_var('EXCHANGE_NAME')
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def consume(self, queue_name, callback):
        if not self.channel:
            raise ConnectionError("Connection is not established.")
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def get(self, queue_name):
        if not self.channel:
            raise ConnectionError("Connection is not established.")
        method_frame, header_frame, body = self.channel.basic_get(queue=queue_name)
        return method_frame, header_frame, body

    def publish(self, queue_name, routing_key, message):
        if not self.channel:
            raise ConnectionError("Connection is not established.")
        self.channel.exchange_declare(exchange=self.exchange_name, durable=True)
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=message
        )
        print(f"Sent message to queue {queue_name}: {message}")
