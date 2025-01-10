from confluent_kafka.admin import AdminClient

# Подключение к Kafka
admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

# Получение списка топиков
topics = admin_client.list_topics()

# Вывод списка топиков
print("Existing topics:", list(topics.topics))





