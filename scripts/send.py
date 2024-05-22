import glob
from rabbit_mq import RabbitMq


class Send(RabbitMq):

    @staticmethod
    def read_file(file_path=''):
        """Reading json format file"""
        with open(file_path, 'rb') as file:
            data = file.read()
        return data

    def send_massage(self, msg):
        """Sending a message to a queue RabbitMQ"""
        connection = self.connect_rabbit()
        self.channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key, body=msg)
        connection.close()

    def main(self, file_path):
        msg = self.read_file(file_path)
        self.send_massage(msg)


if __name__ == '__main__':
    send = Send()
    for filename in glob.iglob('/home/ruscon/sambashare/RabbitMQ/errors/*.json'):
        send.main(filename)
