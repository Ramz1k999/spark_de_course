from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("WeatherKafkaStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

schema = StructType([
    StructField("city",       StringType(),  True),
    StructField("temp_k",     DoubleType(),  True),
    StructField("humidity",   DoubleType(),  True),
    StructField("wind_speed", DoubleType(),  True),
    StructField("weather",    StringType(),  True),
    StructField("timestamp",  LongType(),    True),
])

# Читаем из Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Парсим JSON
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Обогащаем данные
df_enriched = df_parsed \
    .withColumn("temp_c", spark_round(col("temp_k") - 273.15, 1)) \
    .withColumn("ingested_at", current_timestamp())

# Вместо вывода в консоль — пишем в Parquet
query = df_enriched.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "lakehouse/weather/streaming") \
    .option("checkpointLocation", "checkpoints/weather") \
    .start()

query.awaitTermination(timeout=120)

df = spark.read.parquet("lakehouse/weather/streaming")
df.show(truncate=False)
print("Всего записей:", df.count())