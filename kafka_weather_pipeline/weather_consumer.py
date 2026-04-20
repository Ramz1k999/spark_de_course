from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Consumer запущен — ожидаем сообщения...")

for msg in consumer:
    data = msg.value
    temp_c = round(data['temp_k'] - 273.15, 1)
    print(f"{data['city']:<12} | {temp_c}°C | Влажность: {data['humidity']}% | Ветер: {data['wind_speed']} м/с | {data['weather']}")