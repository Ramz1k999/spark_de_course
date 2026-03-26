from pyspark.sql.functions import col, sum as spark_sum, count, broadcast
import time

spark = SparkSession.builder \\
    .appName("Optimization") \\
    .config("spark.sql.adaptive.enabled", "false") \\
    .getOrCreate()

orders = spark.range(500_000).select(
    col("id").alias("order_id"),
    (col("id") % 100).alias("customer_id"),
    (col("id") % 5).alias("country_id"),
    (col("id") % 10).cast("double").alias("amount")
)

customers = spark.createDataFrame(
    [(i, f"Customer_{i}", i % 3) for i in range(100)],
    ["customer_id", "name", "tier"]
)

countries = spark.createDataFrame([
    (0, "UK"), (1, "Germany"), (2, "France"), (3, "Spain"), (4, "Italy")
], ["country_id", "country_name"])

results = orders \\
    .join(broadcast(countries), on="country_id") \\
    .join(broadcast(customers), on="customer_id")

results.cache()

results.groupBy("country_name").agg(spark_sum("amount").alias("total_revenue")).show()
results.groupBy("tier").agg(count("order_id").alias("total_orders")).show()

results.unpersist()