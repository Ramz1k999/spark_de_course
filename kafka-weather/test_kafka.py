from kafka import KafkaProducer, KafkaConsumer
import json

# Producer — отправляем сообщение
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('test-topic', {'message': 'Hello Kafka!', 'city': 'Tashkent'})
producer.flush()
print("Сообщение отправлено ✓")

# Consumer — читаем сообщение
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=3000
)

for msg in consumer:
    print("Получено:", msg.value)