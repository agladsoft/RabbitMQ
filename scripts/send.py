from scripts.rabbit_mq import RabbitMQ


def read_file(file_path='') -> bytes:
    """
    Reading json format file.
    :param file_path: File path.
    :return: File content.
    """
    with open(file_path, 'rb') as file:
        data = file.read()
    return data


if __name__ == '__main__':
    rabbit_mq = RabbitMQ()
    data_file = read_file("/home/timur/PycharmWork/RabbitMQ/test_deal.json")
    rabbit_mq.publish("DC_TEST_Q", "DC_TEST_RT", data_file)

    data_file = read_file("/home/timur/PycharmWork/RabbitMQ/test_deal2.json")
    rabbit_mq.publish("DC_TEST_Q2", "DC_TEST_RT2", data_file)
