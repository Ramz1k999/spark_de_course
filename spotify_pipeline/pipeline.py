from pyspark.sql.functions import count
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, avg,
)


spark = SparkSession.builder.appName("Lakehouse").getOrCreate()

def bronze_layer():
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("spotify_data.csv")
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.mode("overwrite").parquet("lakehouse/bronze")

def silver_layer():
    df = spark.read.parquet("lakehouse/bronze")
    df = df.dropDuplicates(["track_id"])
    df = df.drop("Unnamed: 0")
    df = df.dropna(subset=["track_id", "popularity", "artist_name"])
    df = df.withColumn("popularity", col("popularity").cast("integer"))
    df = df.withColumn("duration_ms", col("duration_ms").cast("integer"))
    df = df.withColumn("duration_min", col("duration_ms") / 60000)
    df.write.mode("overwrite").parquet("lakehouse/silver")

def golden_layer():
    df = spark.read.parquet("lakehouse/silver")

    top_artists = df.groupBy("artist_name") \
        .agg(
        avg("popularity").alias("avg_popularity"),
        count("track_id").alias("track_count")
    ) \
        .filter(col("track_count") >= 10) \
        .orderBy(col("avg_popularity").desc()) \
        .limit(20)
    top_artists.show()

    top_artists.write.mode("overwrite").parquet("lakehouse/gold/top_artists")

    genre_stats = df.groupBy("genre") \
        .agg(
        avg("danceability").alias("avg_danceability"),
        avg("energy").alias("avg_energy"),
        avg("valence").alias("avg_valence")
    )

    genre_stats.show()

    genre_stats.write.mode("overwrite").parquet("lakehouse/gold/genre_stats")

    yearly_trends = df.groupBy("year") \
        .agg(avg("popularity").alias("avg_popularity")) \
        .orderBy("year")
    yearly_trends.show()

    yearly_trends.write.mode("overwrite").parquet("lakehouse/gold/yearly_trends")


if __name__ == "__main__":
    bronze_layer()
    silver_layer()
    golden_layer()
    print("Pipeline завершён ✓")