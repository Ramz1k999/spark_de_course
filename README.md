# Apache Spark Data Engineering Course

Personal learning project — studying Apache Spark for Data Engineering from scratch.

## Stack
- Python 3.12
- Apache Spark 4.0
- PySpark
- Google Colab
- Databricks Community Edition

## Stages

### Stage 1 — Spark Basics
- SparkSession setup
- Creating DataFrames
- Transformations: filter, select, withColumn, groupBy
- Actions vs transformations (lazy evaluation)
- Writing to Parquet

### Stage 2 — ETL & Transformations
- Reading real CSV datasets
- Data cleaning and casting
- Aggregations with groupBy + agg
- Window functions: dense_rank, cumulative sum
- Joins between multiple DataFrames
- Partitioned Parquet writes

### Stage 3 — Structured Streaming
- Reading a stream of JSON files with readStream
- Enriching streaming data with new columns
- Writing stream output to Parquet
- Checkpoint mechanism — fault tolerance
- outputMode: append vs complete

### Stage 4 — Optimization
- Broadcast join for small tables
- Caching with cache() and unpersist()
- Chaining joins across three tables
- Measuring execution time improvements

### Stage 5 — Lakehouse Pipeline (Bronze → Silver → Gold)
- Bronze layer: raw data ingestion with timestamp
- Silver layer: deduplication, filtering, enrichment
- Gold layer: business metrics (top products, revenue by country, avg order)

### Databricks — Delta Lake
- Delta Lake instead of Parquet
- Time Travel (versionAsOf)
- MERGE / Upsert operations
- Table history with DESCRIBE HISTORY

## How to run
1. Open any `.py` file in Google Colab
2. Install dependencies: `!pip install pyspark -q`
3. Run the cells in order

## Dataset
- Stage 2: [Online Retail Dataset](https://github.com/databricks/Spark-The-Definitive-Guide)
- Stages 1, 3, 4, 5: Synthetic data generated in code

## Next steps
- [x] Delta Lake on Databricks Community Edition
- [x] Apache Airflow orchestration
- [ ] Full pipeline on real dataset