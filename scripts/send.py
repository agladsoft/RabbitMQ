import json
from scripts.rabbit_mq import RabbitMQ
from scripts.__init__ import get_my_env_var


if __name__ == '__main__':
    rabbit_mq = RabbitMQ()
    # Читаем исходный json как dict
    with open(f"{get_my_env_var('XL_IDP_ROOT_RABBITMQ')}/config/test_deal.json", 'r', encoding='utf-8') as f:
        base_data = json.load(f)
    for i in range(1, 16):
        # Копируем данные и меняем report
        data = base_data.copy()
        data['header'] = data['header'].copy()
        data['header']['report'] = f"ТЕСТ_{i}"
        # Сериализуем в bytes
        data_bytes = json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8')
        queue_name = f"DC_TEST_QUEUE_{i}"
        routing_key = f"DC_TEST_RT_{i}"
        for _ in range(500):
            rabbit_mq.publish(queue_name, routing_key, data_bytes)
