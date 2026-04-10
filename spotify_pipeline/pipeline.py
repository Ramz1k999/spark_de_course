from pyspark.sql.functions import count
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, avg,
)


spark = SparkSession.builder.appName("Lakehouse").getOrCreate()

def bronze_layer():
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(r"d:\datasets\spotify\spotify_data.csv")
    df = df.withColumn("ingested_at", current_timestamp())
    df.write.mode("overwrite").parquet("lakehouse/bronze")

def run_quality_checks(df: DataFrame, spark: SparkSession , fail_threshold: float = 0.05) -> DataFrame:
    total = df.count()
    logger.info(f"[DQ] Всего строк в Bronze: {total:,}")

    checks = [
        ("track_id", (col("track_id").isNull() | col("track_id") == "")),
        ("artist_name", (col("artist_name").isNull() | col("artist_name") == "")),
        ("popularity", (col("popularity").isNull() | col("popularity") == "")),
        ("genre", (col("genre").isNull() | col("genre") == "")),
        ("year", (col("year").isNull() | col("year") == "")),
        ("popularity", (col("popularity") < 0 | col("popularity") > 100)),
        ("year", (col("year") < 1900 | col("year") > 2024)),
        ("duration_ms", (col("duration_ms") < 0)),
        ("duration_ms", (col("duration_ms") < 30000) | (col("duration_ms") > 3600000)),
        ("danceability", (col("danceability") < 0.0 | col("danceability") > 1.0)),
        ("energy", (col("energy") < 0.0 | col("energy") > 1.0)),
        ("valence", (col("valence") < 0.0 | col("valence") > 1.0)),
        ("duration_check", (col("duration_ms") < col("popularity") * 1000)),
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

    dup_count = (
        df.groupBy("track_id")
        .agg(count("track_id").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    dup_pct = dup_count / total * 100
    dup_status = "FAIL" if dup_pct > fail_threshold * 100 else "WARN" if dup_count > 0 else "OK"
    logger.info(f"[DQ] UNIQUE track_id: {dup_count:,} дублей ({dup_pct:.2f}%) — {dup_status}")
    audit_rows.append(("UNIQUE track_id", dup_count, round(dup_pct, 4), dup_status))

    audit_df = spark.createDataFrame(
        audit_rows,
        ["check_name", "bad_row_count", "bad_row_pct", "status"]
    ).withColumn("checked_at", current_timestamp())
    
    audit_df.write.mode("overwrite").parquet("lakehouse/spotify/audit")
    logger.info("[DQ] Audit log сохранён в lakehouse/spotify/audit")

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

    df = run_quality_checks(df, spark, fail_threshold=0.05)
    df = df.filter(col("_dq_passed")).drop("_dq_passed")
    
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