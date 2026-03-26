from pyspark.sql.functions import col, upper, avg, when

spark = SparkSession.builder.appName("Basics").getOrCreate()

data = [
    (1, "Alice",  "Engineering", 95000),
    (2, "Bob",    "Marketing",   72000),
    (3, "Carol",  "Engineering", 110000),
    (4, "David",  "HR",          58000),
    (5, "Eve",    "Marketing",   85000),
    (6, "Frank",  "Engineering", 102000),
]
columns = ["id", "name", "department", "salary"]
df = spark.createDataFrame(data, columns)

df_with_upper = df.withColumn("name_upper", upper(col("name")))
avg_salary = df.select(avg("salary").alias("avg")).first()["avg"]
df_result = df_with_upper.filter(col("salary") > avg_salary).orderBy(col("salary").desc())
df_result.show()
df_result.write.mode("overwrite").parquet("output/result")