import random
import json
from fastavro import writer
from fastavro.schema import load_schema
from confluent_kafka import Producer
import io
import time

# Загрузка схем
product_schema = load_schema("product.avsc")
purchase_schema = load_schema("purchase.avsc")

# Настройки для подключения к Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Адрес Kafka
}

# Создание продюсера
producer = Producer(conf)

# Генерация тестовых данных для products
def generate_product_data():
    return {
        "id": random.randint(0, 50),
        "name": f"Product-{random.randint(1, 100)}",
        "description": f"Description of Product",
        "price": round(random.uniform(50, 70), 2)
    }

# Генерация тестовых данных для purchases
def generate_purchase_data():
    return {
        "id": random.randint(0, 100),
        "quantity": random.randint(1, 10),
        "productid": random.randint(0, 50)
    }

# Функция для сериализации данных в Avro формат
def serialize_avro_data(data, schema):
    bytes_writer = io.BytesIO()
    writer(bytes_writer, schema, [data])
    return bytes_writer.getvalue()

# Отправка данных в топик 'products'
def send_product_data():
    for _ in range(100):
        product_data = generate_product_data()
        avro_data = serialize_avro_data(product_data, product_schema)
        producer.produce('products', value=avro_data)
        producer.flush()


# Отправка данных в топик 'purchases'
# Осторожно бесконечный цикл!!!
def send_purchase_data():
    while True:
        for _ in range(10):
            purchase_data = generate_purchase_data()
            avro_data = serialize_avro_data(purchase_data, purchase_schema)
            producer.produce('purchases', value=avro_data)
            producer.flush()
        print("Sent 100 purchase messages.")
        time.sleep(1)  # Задержка 2 секунд

# Отправляем данные
send_product_data()
send_purchase_data()

print("Data sent to 'products' and 'purchases' topics in Avro format.")
