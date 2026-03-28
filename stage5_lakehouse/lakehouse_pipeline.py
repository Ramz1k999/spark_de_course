python_round = round

import json, os, random
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, avg,
    sum as spark_sum
)
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Lakehouse").getOrCreate()

# --- Генератор данных ---
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
            "price":       python_round(random.uniform(10.0, 500.0), 2),
            "country":     random.choice(countries),
            "customer_id": f"CUST-{random.randint(1, 100)}"
        })
    with open(f"orders/batch_{batch_id}.json", "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

# --- Схема ---
schema = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("product",     StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       DoubleType(),  True),
    StructField("country",     StringType(),  True),
    StructField("customer_id", StringType(),  True),
])

# --- BRONZE ---
def bronze_layer():
    for i in range(3):
        generate_orders(i)
    df = spark.read.schema(schema).json("orders/")
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.mode("overwrite").parquet("lakehouse/bronze")
    print("Bronze готов ✓")

# --- SILVER ---
def silver_layer():
    df = spark.read.parquet("lakehouse/bronze")
    df = df.dropDuplicates(["order_id"])
    df = df.filter(col('quantity') > 0).filter(col('price') > 0)
    df = df.withColumn("total_amount", col('quantity') * col('price'))
    df.show(5)
    df.write.mode("overwrite").parquet("lakehouse/silver")
    print("Silver готов ✓")

# --- GOLD ---
def gold_layer():
    df = spark.read.parquet("lakehouse/silver")

    top_products = df.groupBy("product") \
        .agg(spark_sum('total_amount').alias('total_revenue')) \
        .orderBy(col('total_revenue').desc()) \
        .limit(10)
    top_products.show()
    top_products.write.mode("overwrite").parquet("lakehouse/gold/top_products")

    top_country = df.groupBy("country") \
        .agg(spark_sum('total_amount').alias('total_revenue'))
    top_country.show()
    top_country.write.mode("overwrite").parquet("lakehouse/gold/revenue_by_country")

    avg_order = df.groupBy("country") \
        .agg(avg('total_amount').alias('avg_order'))
    avg_order.show()
    avg_order.write.mode("overwrite").parquet("lakehouse/gold/avg_order_by_country")
    print("Gold готов ✓")

# --- Запуск ---
bronze_layer()
silver_layer()
gold_layer()
print("Pipeline завершён ✓")