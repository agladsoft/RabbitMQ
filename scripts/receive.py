import json
import contextlib
from app_logger import *
from pathlib import Path
from __init__ import RabbitMq
from datetime import datetime
from clickhouse import ClickHouse


logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))
date_formats: tuple = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z")


class Receive(RabbitMq):

    def read_msg(self):
        ''' Connecting to a queue and receiving messages '''
        logger.info('Connect')
        channel, connection = self.connect_rabbit()
        channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=self.durable)
        channel.queue_declare(queue=self.queue_name,durable=self.durable)
        channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        ''' Working with the message body'''
        logger.info('Get body message')
        self.save_text_msg(body)
        self.read_json(body)
        # ch.basic_ack(delivery_tag=method.delivery_tag)

    def save_text_msg(self, msg):
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/msg/{datetime.now()}-text_msg.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg.decode('utf-8-sig'), file, indent=4, ensure_ascii=False)

    def change_columns(self, data):
        voyageDate = data.get('voyageDate')
        if voyageDate is not None:
            data['voyageDate'] = self.convert_format_date(voyageDate)
        containerCount = data.get('containerCount')
        if containerCount is not None:
            data['containerCount'] = int(containerCount)
        containerSize = data.get('containerSize')
        if containerSize is not None:
            data['containerSize'] = int(containerSize)


    @staticmethod
    def convert_format_date(date: str) -> str:
        """
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return date

    def read_json(self, msg):
        ''' Decoding a message and working with data'''
        logger.info('Read json')
        msg = msg.decode('utf-8-sig')
        clickhouse = ClickHouse(logger)
        file_name = f"data_core_{datetime.now()}.json"
        clickhouse.delete_deal()
        data = json.loads(msg)
        for n, d in enumerate(data):
            self.add_new_columns(d, file_name)
            self.change_columns(d)
            self.write_to_json(d, n)
        clickhouse.count_number_loaded_rows(data, len(data), file_name)

    def add_new_columns(self, data, file_name):
        ''' Adding new columns '''
        data['original_file_parsed_on'] = file_name
        data['is_obsolete'] = None
        data['is_obsolete_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, msg, en):
        ''' Write data to json file '''
        file_name: str = f"{get_my_env_var('XL_IDP_PATH_RABBITMQ')}/json/{en}-{datetime.now()}.json"
        fle: Path = Path(file_name)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        with open(file_name, 'w') as file:
            json.dump(msg, file, indent=4, ensure_ascii=False)

    def main(self):
        logger.info('Start read')
        self.read_msg()
        logger.info('End read')


if __name__ == '__main__':
    Receive().main()
