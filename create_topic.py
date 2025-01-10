from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Подключение к Kafka
admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test-client')

# Создание топиков
topic_list = [NewTopic(name="products", num_partitions=1, replication_factor=1),
              NewTopic(name="purchases", num_partitions=1, replication_factor=1)]

admin_client.create_topics(new_topics=topic_list, validate_only=False)
