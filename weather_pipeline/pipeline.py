from pandas import DataFrame
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


def run_quality_checks(df: DataFrame, spark: SparkSession , fail_threshold: float = 0.0) -> DataFrame:
    total = df.count()
    logger.info(f"[DQ] Всего строк в Bronze: {total:,}")

    checks = [
        ("city", (col("city").isNull() | col("city") == "")),
        ("temp_k", (col("temp_k").isNull())),
        ("humidity", (col("humidity").isNull())),
        ("wind_speed", (col("wind_speed").isNull())),
        ("weather", (col("weather").isNull())),
        ("temp_k", (col("temp_k") < 180 | col("temp_k") > 340)),
        ("humidity", (col("humidity") < 0 | col("humidity") > 100)),
        ("wind_speed", (col("wind_speed") < 0 | col("wind_speed") > 113)),
        ("empty_response", (col("temp_k") == 0) & (col("humidity") == 0)),  
    ]

    audit_rows = []
    failed_mask = lit(False)

    for check_name, bad_condition in checks:
        bad_count = df.filter(bad_condition).count()
        pct = bad_count / total * 100
        status = "FAIL" if pct > fail_threshold * 100 else "WARN" if bad_count > 0 else "OK"
        
        logger.info(f"[DQ] {check_name}: {bad_count:,} строк ({pct:.2f}%) — {status}")
        audit_rows.append((check_name, bad_count, round(pct, 4), status))
        
        # Помечаем строку как плохую если нарушает хоть одно правило
        failed_mask = failed_mask | bad_condition

    # Сохраняем audit log
    audit_df = spark.createDataFrame(
        audit_rows,
        ["check_name", "bad_row_count", "bad_row_pct", "status"]
    ).withColumn("checked_at", current_timestamp())
    
    audit_df.write.mode("overwrite").parquet("lakehouse/weather/audit")
    logger.info("[DQ] Audit log сохранён в lakehouse/weather/audit")

    # Считаем общий процент плохих строк
    df = df.withColumn("_dq_passed", ~failed_mask)
    total_bad = df.filter(~col("_dq_passed")).count()
    total_bad_pct = total_bad / total

    if total_bad_pct > fail_threshold:
        raise ValueError(
            f"[DQ] PIPELINE STOPPED: {total_bad:,} плохих строк ({total_bad_pct*100:.1f}%) "
            f"превышает порог {fail_threshold*100}%"
        )

    logger.info(f"[DQ] Проверка пройдена. Плохих строк: {total_bad:,} ({total_bad_pct*100:.1f}%)")
    return df
    

def silver_layer():
    df = spark.read.parquet("lakehouse/weather/bronze")
    
    df = run_quality_checks(df, spark, fail_threshold=0.0)
    df = df.filter(col("_dq_passed")).drop("_dq_passed")
    
    df = df.withColumn("temperature_c", col("temp_k") - 273.15)
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