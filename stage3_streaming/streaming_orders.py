from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp, sum as spark_sum
import json, os, random

spark = SparkSession.builder.appName("StreamingOrders").getOrCreate()

os.makedirs("orders", exist_ok=True)
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]
countries = ["UK", "Germany", "France", "Spain", "Italy"]

def generate_orders(batch_id, n=5):
    orders = []
    for i in range(n):
        orders.append({
            "order_id":    f"ORD-{batch_id}-{i}",
            "product":     random.choice(products),
            "quantity":    random.randint(1, 10),
            "price":       round(random.uniform(10.0, 500.0), 2),
            "country":     random.choice(countries),
            "customer_id": f"CUST-{random.randint(1, 100)}"
        })
    with open(f"orders/batch_{batch_id}.json", "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\\n")

for i in range(3):
    generate_orders(i)

schema = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("product",     StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       DoubleType(),  True),
    StructField("country",     StringType(),  True),
    StructField("customer_id", StringType(),  True),
])

streaming_df = spark.readStream.schema(schema).json("orders/")
streaming_df_enriched = streaming_df \\
    .withColumn("total_amount", col("quantity") * col("price")) \\
    .withColumn("ingested_at", current_timestamp())

query = streaming_df_enriched.writeStream \\
    .format("parquet") \\
    .outputMode("append") \\
    .option("checkpointLocation", "checkpoints/orders") \\
    .start("output/orders")

query.awaitTermination(timeout=20)
result = spark.read.parquet("output/orders")
result.groupBy("country").agg(spark_sum("total_amount").alias("total_revenue")).orderBy(col("total_revenue").desc()).show()