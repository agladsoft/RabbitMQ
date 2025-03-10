from scripts.rabbit_mq import RabbitMQ
from scripts.__init__ import get_my_env_var


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
    data_file = read_file(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/config/test_deal.json")
    rabbit_mq.publish("DC_ACCOUNTING_DOCUMENTS_REQUESTS_QUEUE_TEST", "DC_ACCOUNTING_DOCUMENTS_REQUESTS_RT", data_file)
