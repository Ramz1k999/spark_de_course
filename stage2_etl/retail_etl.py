from pyspark.sql.functions import col, sum as spark_sum, avg, count, round as spark_round, dense_rank, desc
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RetailETL").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("online-retail-dataset.csv")

df_clean = df \\
    .filter(col("Quantity") > 0) \\
    .filter(col("UnitPrice") > 0) \\
    .filter(col("CustomerID").isNotNull()) \\
    .withColumn("Quantity",  col("Quantity").cast("integer")) \\
    .withColumn("UnitPrice", col("UnitPrice").cast("double")) \\
    .withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

top_5_customers = df_clean.groupBy("CustomerID") \\
    .agg(spark_sum("TotalPrice").alias("TotalSum")) \\
    .orderBy(col("TotalSum").desc()) \\
    .limit(5)

window_spec = Window.partitionBy("CustomerID") \\
    .orderBy("InvoiceDate") \\
    .rowsBetween(Window.unboundedPreceding, 0)

df_with_cumulative = df_clean.withColumn(
    "CumulativeSum",
    spark_sum("TotalPrice").over(window_spec)
)

result_df = df_with_cumulative.join(
    top_5_customers.select("CustomerID"),
    on="CustomerID",
    how="inner"
)

result_df.write.mode("overwrite").partitionBy("Country").parquet("output/top_customers")