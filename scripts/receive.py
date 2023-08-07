import json
import os

from __init__ import RabbitMq
from datetime import datetime




class Receive(RabbitMq):

    def read_msg(self):
        ''' Connecting to a queue and receiving messages '''
        channel, connection = self.connect_rabbit()
        channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        channel.queue_declare(queue=self.queue_name)
        channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        ''' Working with the message body'''
        self.read_json(body)

    def read_json(self, msg):
        ''' Decoding a message and working with data'''
        data = json.loads(msg.decode('utf-8-sig'))
        for n, d in enumerate(data):
            self.add_new_columns(d)
            self.write_to_json(d, n)


    def add_new_columns(self, data):
        ''' Adding new columns '''
        data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, msg, en):
        ''' Write data to json file '''
        with open(f"{os.environ.get('XL_IDP_ROOT_RABBITMQ')}/{en}.json", 'w') as file:
            json.dump(msg, file)

    def main(self):
        self.read_msg()


if __name__ == '__main__':
    Receive().main()
