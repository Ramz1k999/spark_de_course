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