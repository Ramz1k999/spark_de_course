from pyspark.sql.functions import col, current_timestamp, avg, count, max
from pyspark.sql.functions import unix_timestamp, hour, dayofweek
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Lakehouse").getOrCreate()

def bronze_layer():
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(r"d:\datasets\nyctaxi\yellow_tripdata_2016-01.csv")
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.mode("overwrite").parquet("lakehouse/bronze")

def silver_layer():
    df = spark.read.parquet("lakehouse/bronze")
    df = df.filter(col('total_amount') > 0).filter(col('trip_distance') > 0).filter(col('passenger_count') > 0)
    df = df.withColumn(
    "trip_duration_min",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)
    df = df.withColumn("hour_of_day", hour("tpep_pickup_datetime"))
    df = df.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))

    df.write.mode("overwrite").parquet("lakehouse/silver")

def gold_layer():
    df = spark.read.parquet("lakehouse/silver")
    daily_income = df.groupBy("hour_of_day").agg(spark_sum("total_amount").alias("total_revenue"), count("*").alias("trip_count")).orderBy("hour_of_day")
    daily_income.write.mode("overwrite").parquet("lakehouse/gold/daily_income")

    avg_distance = df.groupBy("day_of_week").agg(avg("trip_distance").alias("avg_distance"), avg("tip_amount").alias("avg_tip")).orderBy("day_of_week")
    avg_distance.write.mode("overwrite").parquet("lakehouse/gold/avg_distance")

    top_10 = df.orderBy(col("total_amount").desc()).limit(10)
    top_10.write.mode("overwrite").parquet("lakehouse/gold/top_10_income")

def main():
    bronze_layer()
    silver_layer()
    gold_layer()

if __name__ == "__main__":
    main()