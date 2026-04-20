from kafka import KafkaProducer
import requests
import json
import time

API_KEY = "198c298e56826728b98a55e7a80a461e"
CITIES = ["Tashkent", "Moscow", "London", "New York", "Tokyo"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send():
    for city in CITIES:
        r = requests.get(
            f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}'
        )
        data = r.json()

        # Проверяем что запрос успешный
        if data.get("cod") != 200:
            print(f"Ошибка для {city}: {data}")
            continue

        message = {
            "city": data["name"],
            "temp_k": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "weather": data["weather"][0]["description"],
            "timestamp": data["dt"]
        }

        producer.send("weather-topic", message)
        print(f"Отправлено: {message['city']} — {message['temp_k']}K")

    producer.flush()
    print("Все города отправлены ✓")

if __name__ == "__main__":
    while True:
        fetch_and_send()
        print("Ждём 60 секунд...")
        time.sleep(60)