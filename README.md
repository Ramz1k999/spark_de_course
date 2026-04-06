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
- Requests (API integration)

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
- Flexible scheduling (daily and 10-minute intervals)
- Docker deployment
- Task dependencies and error handling

## Projects

### Project 1 — Spotify Tracks Pipeline
- Dataset: 1.1M Spotify tracks from Kaggle
- Bronze: raw CSV ingestion with timestamp
- Silver: deduplication, type casting, null removal, duration in minutes
- Gold: top 20 artists by popularity, genre stats (danceability/energy/valence), yearly trends
- Orchestrated with Apache Airflow DAG

### Project 2 — NYC Taxi Pipeline
- Dataset: 10.9M NYC Yellow Taxi trips (January 2016)
- Bronze: raw CSV ingestion with timestamp
- Silver: filtering, trip duration, hour of day, day of week
- Gold: hourly revenue, weekly distance/tips, top 10 expensive trips
- Local execution with PySpark on Windows

### Project 3 — Real-time Weather Pipeline
- **API Source:** OpenWeatherMap API (5 cities: Tashkent, Moscow, London, New York, Tokyo)
- **Architecture:** Bronze → Silver → Gold with Airflow orchestration
- **Bronze:** Raw JSON ingestion from REST API with ingestion timestamp
- **Silver:** Data cleaning (null removal), unit conversion (Kelvin → Celsius), validation
- **Gold:** Temperature comparison (ranking), wind speed analysis, extreme weather filtering
- **Orchestration:** Apache Airflow DAG running every 10 minutes (`*/10 * * * *`)
- **Stack:** PySpark + Requests + Airflow

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
├── spotify_pipeline/
│   ├── pipeline.py
│   └── dag.py
├── nyc_taxi_pipeline/
│   └── pipeline.py
└── weather_pipeline/
    ├── pipeline.py          # Bronze/Silver/Gold layers
    └── _dag.py       # Airflow DAG (10 min schedule)
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

### NYC Taxi Pipeline (local)
1. Install dependencies: `pip install pyspark`
2. Download dataset from Kaggle: NYC Yellow Taxi Trip Data
3. Run: `python pipeline.py`

### Weather Pipeline (API + Airflow)
1. Get free API key from [OpenWeatherMap](https://openweathermap.org/api)
2. Install dependencies: `pip install pyspark requests`
3. **Local test:** `python weather_pipeline/pipeline.py`
4. **Airflow (Docker):**
   - Add API key to environment variables or config
   - Place `weather_dag.py` in `dags/` folder
   - Run: `docker compose up -d`
   - Open: http://localhost:8080
   - Enable DAG `weather_pipeline` (runs every 10 minutes)

### Airflow (General)
1. Install Docker Desktop
2. Run: `docker compose up -d`
3. Open: http://localhost:8080 (login: airflow / airflow)
4. Enable and trigger desired DAG (Spotify or Weather)

## Dataset Sources
- Stage 2: [Online Retail Dataset](https://github.com/databricks/Spark-The-Definitive-Guide)
- Stages 1, 3, 4, 5: Synthetic data generated in code
- Project 1: [Spotify Tracks Dataset](https://www.kaggle.com/datasets/amitanshjoshi/spotify-1million-tracks)
- Project 2: [NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- Project 3: [OpenWeatherMap API](https://openweathermap.org/api) (Real-time data)

## Next steps
- [x] Delta Lake on Databricks Community Edition
- [x] Apache Airflow orchestration
- [x] Full pipeline on real dataset (Spotify)
- [x] Large-scale pipeline (NYC Taxi — 10.9M rows)
- [x] Real-time API ingestion (Weather pipeline)
- [ ] Cloud deployment (AWS S3 + EMR or GCP Dataproc)
- [ ] dbt for data transformation layer
- [ ] Real-time streaming with Kafka
- [ ] Data quality checks (Great Expectations)

