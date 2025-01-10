from fastavro import writer
from fastavro.schema import load_schema
from confluent_kafka import Producer
import io

# Загрузка схем
product_schema = load_schema("product.avsc")
purchase_schema = load_schema("purchase.avsc")

# Настройки для Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
}

# Создание продюсера
producer = Producer(conf)

# Функция для сериализации данных в Avro формат
def serialize_avro_data(data, schema):
    bytes_writer = io.BytesIO()
    writer(bytes_writer, schema, [data])
    return bytes_writer.getvalue()

# Данные для топика 'products'
products = [
    {"id": 1, "name": "Product-0", "description": "high price", "price": 1000.0},
    {"id": 2, "name": "Product-1", "description": "low price", "price": 10.0},
]

# Данные для топика 'purchases'
purchases = [
    {"id": 101, "quantity": 4, "productid": 1},  # Подходит под алерт
    {"id": 102, "quantity": 2, "productid": 2},  # Не подходит под алерт
]

# Отправка данных в топик 'products'
for product in products:
    avro_data = serialize_avro_data(product, product_schema)
    producer.produce('products', value=avro_data)

# Отправка данных в топик 'purchases'
for purchase in purchases:
    avro_data = serialize_avro_data(purchase, purchase_schema)
    producer.produce('purchases', value=avro_data)

# Подтверждение отправки
producer.flush()
print("Test data sent to Kafka topics 'products' and 'purchases'.")
