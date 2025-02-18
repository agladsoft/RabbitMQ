import pika
from typing import Tuple
from scripts.__init__ import get_my_env_var


class RabbitMQ:
    def __init__(self):
        self.user: str = get_my_env_var('RABBITMQ_USER')
        self.password: str = get_my_env_var('RABBITMQ_PASSWORD')
        self.host: str = get_my_env_var('RABBITMQ_HOST')
        self.port: int = int(get_my_env_var('RABBITMQ_PORT'))
        self.exchange_name: str = get_my_env_var('EXCHANGE_NAME')
        self.connection, self.channel = self.connect()

    def connect(self) -> Tuple[pika.BlockingConnection, pika.adapters.blocking_connection.BlockingChannel]:
        """
        Establishes a connection to the RabbitMQ server using the provided
        credentials and connection parameters. Returns a tuple containing
        the connection and the channel objects.
        :return: A tuple containing the connection and the channel.
        """
        credentials: pika.PlainCredentials = pika.PlainCredentials(self.user, self.password)
        parameters: pika.ConnectionParameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=600,
            connection_attempts=5,
            blocked_connection_timeout=300,
            retry_delay=3
        )
        connection: pika.BlockingConnection = pika.BlockingConnection(parameters)
        return connection, connection.channel()

    def close(self) -> None:
        """
        Closes the connection to the RabbitMQ server if it is currently open.
        This ensures that resources are properly released and the connection
        is not left hanging. If the connection is already closed, this method
        does nothing.
        :return:
        """
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def declare_and_bind_queue(self, queue_name: str, routing_key: str) -> None:
        """
        Создаёт очередь (если её нет) и привязывает её к exchange.

        :param queue_name: Название очереди.
        :param routing_key: Routing key для привязки.
        """
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=queue_name,
            routing_key=routing_key
        )

    def consume(self, queue_name: str, callback: callable) -> None:
        """
        Subscribes to messages in a RabbitMQ queue and calls the provided callback function
        whenever a message is received. The callback function should take three arguments:
        the method frame, the header frame, and the body of the message.

        :param queue_name: The name of the queue to subscribe to.
        :param callback: The callback function to call when a message is received.
        :return:
        """
        if not self.channel:
            raise ConnectionError("Connection is not established.")
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

    def get(self, queue_name: str) -> Tuple[pika.spec.Basic.Deliver, pika.spec.BasicProperties, bytes]:
        """
        Retrieves a message from a RabbitMQ queue.

        :param queue_name: The name of the queue from which to retrieve the message.
        :return: A tuple containing the method frame, header frame, and body of the message.
        """
        if not self.channel:
            raise ConnectionError("Connection is not established.")
        method_frame, header_frame, body = self.channel.basic_get(queue=queue_name, auto_ack=False)
        return method_frame, header_frame, body

    def publish(self, queue_name: str, routing_key: str, message: bytes) -> None:
        """
        Publishes a message to a RabbitMQ queue.

        :param queue_name: The name of the queue to which to publish the message.
        :param routing_key: The routing key to use when publishing the message.
        :param message: The message to publish to the queue.
        :return:
        """
        if not self.channel:
            raise ConnectionError("Connection is not established.")

        # Объявляем очередь и привязываем её перед отправкой
        self.declare_and_bind_queue(queue_name, routing_key)

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=message
        )
        print(f"Sent message to queue {queue_name}: {message}")
