import pika
from typing import Optional
from __init__ import get_my_env_var
from pika.adapters.blocking_connection import BlockingChannel


class RabbitMq:
    def __init__(self):
        self.user = 'rabbitmq'
        self.host = '10.23.4.199'
        self.password = '8KZ3wXA5W2rP'
        self.exchange = get_my_env_var('EXCHANGE')
        self.routing_key = get_my_env_var('ROUTING_KEY')
        self.durable = True
        self.queue_name = get_my_env_var('QUEUE_NAME')
        self.channel: Optional[BlockingChannel] = None

    def connect_rabbit(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=5672,
            virtual_host='/',
            credentials=credentials,
            heartbeat=600,
            connection_attempts=5,
            blocked_connection_timeout=300,
            retry_delay=3
        )
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        return connection
