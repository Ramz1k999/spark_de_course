from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def run_bronze(): 
    subprocess.run(["python", "pipeline.py", "bronze"], check=True)

def run_silver(): 
    subprocess.run(["python", "pipeline.py", "silver"], check=True)

def run_gold():   
    subprocess.run(["python", "pipeline.py", "gold"], check=True)

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Weather Bronze → Silver → Gold (every 10 min)",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "weather"],
) as dag:

    bronze = PythonOperator(task_id="bronze_layer", python_callable=run_bronze)
    silver = PythonOperator(task_id="silver_layer", python_callable=run_silver)
    gold   = PythonOperator(task_id="gold_layer",   python_callable=run_gold)

    bronze >> silver >> gold


col_name = "tip_amount"

bad_condition = col(col_name).isNull() | col(col_name) < 0

bad_condition = bad_condition | col("tip_amount") > col("fare_amount") * 0.5

bad_count = df.filter(bad_condition).count()
bad_pct = bad_count / total * 100
status    = "OK" if bad_count == 0 else "WARN" if bad_pct < 5 else "FAIL"