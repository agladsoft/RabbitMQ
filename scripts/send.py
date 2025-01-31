from rabbit_mq import RabbitMQ


def read_file(file_path=''):
    """Reading json format file"""
    with open(file_path, 'rb') as file:
        data = file.read()
    return data


if __name__ == '__main__':
    rabbit_mq = RabbitMQ()
    data_file = read_file("/home/timur/PycharmWork/RabbitMQ/test_deal.json")
    rabbit_mq.publish("DC_TEST_Q2", "DC_TEST_RT2", data_file)
