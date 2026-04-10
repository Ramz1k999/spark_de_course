from pyspark.sql.functions import col, current_timestamp, avg, count, max
from pyspark.sql.functions import unix_timestamp, hour, dayofweek
from pyspark.sql.functions import sum as spark_sum, when, lit
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("Lakehouse").getOrCreate()


def bronze_layer():
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(
        r"d:\datasets\nyctaxi\yellow_tripdata_2016-01.csv"
    )
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.mode("overwrite").parquet("lakehouse/bronze")


def run_quality_checks(df: DataFrame, fail_threshold: float = 0.05) -> DataFrame:
    total = df.count()
    logger.info(f"[DQ] Всего строк в Bronze: {total:,}")

    checks = [
        ("negative_total_amount",   col("total_amount") <= 0),
        ("zero_trip_distance",      col("trip_distance") <= 0),
        ("invalid_passenger_count", col("passenger_count") <= 0),
        ("negative_trip_duration",  
            unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime") <= 0),
        ("null_pickup_datetime",    col("tpep_pickup_datetime").isNull()),
        ("null_dropoff_datetime",   col("tpep_dropoff_datetime").isNull()),
        ("extreme_trip_duration",   
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60 > 300),
        ("extreme_fare",            col("fare_amount") > 500),
    ]

    audit_rows = []
    failed_mask = lit(False)

    for check_name, bad_condition in checks:
        bad_count = df.filter(bad_condition).count()
        pct = bad_count / total * 100
        status = "FAIL" if pct > fail_threshold * 100 else "WARN" if bad_count > 0 else "OK"
        
        logger.info(f"[DQ] {check_name}: {bad_count:,} строк ({pct:.2f}%) — {status}")
        audit_rows.append((check_name, bad_count, round(pct, 4), status))
        
        failed_mask = failed_mask | bad_condition

    audit_df = spark.createDataFrame(
        audit_rows,
        ["check_name", "bad_row_count", "bad_row_pct", "status"]
    ).withColumn("checked_at", current_timestamp())
    
    audit_df.write.mode("overwrite").parquet("lakehouse/audit/nyc_taxi_dq")
    logger.info("[DQ] Audit log сохранён в lakehouse/audit/nyc_taxi_dq")

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
    df = spark.read.parquet("lakehouse/bronze")
    
    # Запускаем quality checks — получаем df с колонкой _dq_passed
    df = run_quality_checks(df, fail_threshold=0.05)
    
    # В Silver берём только прошедшие проверку строки
    df = df.filter(col("_dq_passed")).drop("_dq_passed")

    df = df.withColumn(
        "trip_duration_min",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    )
    df = df.withColumn("hour_of_day", hour("tpep_pickup_datetime"))
    df = df.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))

    df.write.mode("overwrite").parquet("lakehouse/silver")


def gold_layer():
    df = spark.read.parquet("lakehouse/silver")

    daily_income = df.groupBy("hour_of_day").agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("*").alias("trip_count")
    ).orderBy("hour_of_day")
    daily_income.write.mode("overwrite").parquet("lakehouse/gold/daily_income")

    avg_distance = df.groupBy("day_of_week").agg(
        avg("trip_distance").alias("avg_distance"),
        avg("tip_amount").alias("avg_tip")
    ).orderBy("day_of_week")
    avg_distance.write.mode("overwrite").parquet("lakehouse/gold/avg_distance")

    top_10 = df.orderBy(col("total_amount").desc()).limit(10)
    top_10.write.mode("overwrite").parquet("lakehouse/gold/top_10_income")


def main():
    bronze_layer()
    silver_layer()
    gold_layer()


if __name__ == "__main__":
    main()