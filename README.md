# Apache Spark Data Engineering Course

Personal learning project — studying Apache Spark for Data Engineering from scratch.

## Stack
- Python 3.12
- Apache Spark 4.0
- PySpark
- Google Colab
- Databricks Community Edition
- Apache Airflow 2.9.1
- Docker

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

### Airflow — Orchestration
- DAG with Bronze → Silver → Gold pipeline
- PythonOperator for task execution
- Daily schedule (@daily)
- Docker deployment

### Final Project — Spotify Tracks Pipeline
- Dataset: 1.1M Spotify tracks from Kaggle
- Bronze: raw CSV ingestion with timestamp
- Silver: deduplication, type casting, null removal, duration in minutes
- Gold: top 20 artists by popularity, genre stats (danceability/energy/valence), yearly trends
- Orchestrated with Apache Airflow DAG

## Project Structure
```
spark-de-course/
├── stage1_basics/
│   └── basics.py
├── stage2_etl/
│   └── retail_etl.py
├── stage3_streaming/
│   └── streaming_orders.py
├── stage4_optimization/
│   └── optimization.py
├── stage5_lakehouse/
│   └── lakehouse_pipeline.py
└── spotify_pipeline/
    ├── pipeline.py        # PySpark ETL
    └── dag.py             # Airflow DAG
```

## How to run

### Stages 1–5 (Google Colab)
1. Open any `.py` file in Google Colab
2. Install dependencies: `!pip install pyspark -q`
3. Run the cells in order

### Spotify Pipeline (local)
1. Install dependencies: `pip install pyspark`
2. Download dataset from Kaggle: Spotify Tracks Dataset
3. Run: `python pipeline.py`

### Airflow (Docker)
1. Install Docker Desktop
2. Run: `docker compose up -d`
3. Open: http://localhost:8080 (login: airflow / airflow)
4. Enable and trigger `spotify_pipeline` DAG

## Dataset Sources
- Stage 2: [Online Retail Dataset](https://github.com/databricks/Spark-The-Definitive-Guide)
- Stages 1, 3, 4, 5: Synthetic data generated in code
- Final Project: [Spotify Tracks Dataset](https://www.kaggle.com/datasets/amitanshjoshi/spotify-1million-tracks)

## Next steps
- [x] Delta Lake on Databricks Community Edition
- [x] Apache Airflow orchestration
- [x] Full pipeline on real dataset
- [ ] Cloud deployment (AWS S3 + EMR or GCP Dataproc)
- [ ] dbt for data transformation layer
- [ ] Real-time streaming with Kafka