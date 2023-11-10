import pika


class RabbitMq:
    def __init__(self):
        self.user = 'rabbitmq'
        self.host = '10.23.4.199'
        self.password = '8KZ3wXA5W2rP'
        self.exchange = 'DC_TEST_EX'
        self.routing_key = 'DC_TEST_RT'
        self.durable = True
        self.queue_name = 'DC_TEST_Q'
        self.time_sleep = 10

    def connect_rabbit(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.host,
                                               5672,
                                               '/',
                                               credentials, heartbeat=18000)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        return channel, connection
