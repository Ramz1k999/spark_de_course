import requests
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("WeatherPipeline").getOrCreate()

API_KEY = "198c298e56826728b98a55e7a80a461e"
CITIES = ["Tashkent", "Moscow", "London", "New York", "Tokyo"]

def bronze_layer():
    weather_data = []

    for city in CITIES:
        r = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}')
        data = r.json()

        city_name = data["name"]
        temp_k = data["main"]["temp"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        weather = data["weather"][0]["description"]

        weather_data.append({
            "city": city_name,
            "temp_k": temp_k,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "weather": weather
        })
        
    df = spark.createDataFrame(weather_data)
    
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.mode("overwrite").parquet("lakehouse/weather/bronze")

def silver_layer():
    df = spark.read.parquet("lakehouse/weather/bronze")
    df = df.dropna(subset=["temp_k", "humidity", "wind_speed"])
    df = df.filter(col("temp_k") > 0)
    df = df.withColumn("temperature_c", col("temp_k") - 273.15)
    df = df.filter(col("wind_speed") >= 0)

    df.write.mode("overwrite").parquet("lakehouse/weather/silver")

def gold_layer():
    df = spark.read.parquet("lakehouse/weather/silver")
    
    temp_comparison = df.select("city", "temperature_c", "weather").orderBy(col("temperature_c").desc())
    temp_comparison.show()
    temp_comparison.write.mode("overwrite").parquet("lakehouse/weather/gold/temp_comparison")
    
    wind_comparison = df.select("city", "wind_speed", "weather").orderBy(col("wind_speed").desc())
    wind_comparison.show()
    wind_comparison.write.mode("overwrite").parquet("lakehouse/weather/gold/wind_comparison")
    
    windy_cities = df.filter(col("wind_speed") > 5)
    windy_cities.show()
    windy_cities.write.mode("overwrite").parquet("lakehouse/weather/gold/windy_cities")

def main():
    if len(sys.argv) > 1:
        layer = sys.argv[1]
        if layer == "bronze":
            bronze_layer()
        elif layer == "silver":
            silver_layer()
        elif layer == "gold":
            gold_layer()
        else:
            print(f"Unknown layer: {layer}")
    else:

        bronze_layer()
        silver_layer()
        gold_layer()

if __name__ == "__main__":
    main()