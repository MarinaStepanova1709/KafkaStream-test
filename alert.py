import time
from confluent_kafka import Consumer, KafkaException, KafkaError
from fastavro import reader
import io
from fastavro.schema import load_schema
from collections import defaultdict

# Загрузка схем
product_schema = load_schema("product.avsc")
purchase_schema = load_schema("purchase.avsc")

# Настройки для подключения к Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer-group',  # Уникальная группа для потребителей
    'auto.offset.reset': 'earliest',  # Начать с самых ранних сообщений
    'enable.auto.commit': True,  # Включаем автоматическое подтверждение
}

# Создание Consumer
consumer = Consumer(conf)

# Подписка на топики
consumer.subscribe(['products', 'purchases'])

# Словарь для хранения данных о покупках
product_data = defaultdict(lambda: {'price': 0, 'total_sales': 0, 'purchases': [], 'last_total_sales': 0})


def deserialize_avro_data(avro_data, schema):
    bytes_reader = io.BytesIO(avro_data)
    avro_reader = reader(bytes_reader)
    return next(avro_reader)


def process_alerts():
    current_time = time.time()  # Получаем текущее время (в секундах с начала эпохи)

    # Проверяем данные о покупках для всех продуктов
    for product_id, data in product_data.items():
        total_purchase_value = 0

        # Фильтруем покупки, сделанные за последние 60 секунд
        recent_purchases = [purchase for purchase in data['purchases'] if current_time - purchase['timestamp'] <= 60]

        # Рассчитываем сумму покупок за последние 60 секунд
        for purchase in recent_purchases:
            total_purchase_value += purchase['quantity'] * data['price']

        # Если сумма покупок превышает 3000, выводим алерт, только если она изменилась
        if total_purchase_value > 3000 and total_purchase_value != data['last_total_sales']:
            print(f"ALERT: product_id = {product_id}: total sales  =  {total_purchase_value} in 60 sec!")
            data['last_total_sales'] = total_purchase_value  # Обновляем сохраненную сумму


# Прослушивание топиков
try:
    while True:
        msg = consumer.poll(1.0)  # Ожидаем сообщения с таймаутом в 1 секунду
        if msg is None:
            continue  # Нет сообщений, продолжаем прослушивание
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # Конец партиции, продолжаем слушать
            else:
                raise KafkaException(msg.error())
        else:
            # Обрабатываем сообщение
            topic = msg.topic()
            avro_data = msg.value()

            if topic == 'products':
                try:
                    product = deserialize_avro_data(avro_data, product_schema)
                    product_data[product['id']]['price'] = product['price']
                except Exception as e:
                    print(f"Error deserializing product data: {e}")

            elif topic == 'purchases':
                try:
                    purchase = deserialize_avro_data(avro_data, purchase_schema)
                    product_id = purchase['productid']
                    purchase['timestamp'] = time.time()  # Добавляем временную метку для каждой покупки
                    product_data[product_id]['purchases'].append(purchase)
                except Exception as e:
                    print(f"Error deserializing purchase data: {e}")

            # После каждого сообщения проверяем, если сумма превышает 3000 за последние 60 секунд
            process_alerts()

except KeyboardInterrupt:
    print("Listening interrupted.")

finally:
    # Закрытие consumer
    consumer.close()
