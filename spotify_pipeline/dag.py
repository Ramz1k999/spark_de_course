from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def run_bronze(): subprocess.run(["python", "pipeline.py", "bronze"])
def run_silver(): subprocess.run(["python", "pipeline.py", "silver"])
def run_gold():   subprocess.run(["python", "pipeline.py", "gold"])

with DAG(
    dag_id="spotify_pipeline",
    default_args=default_args,
    description="Spotify Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "spotify"],
) as dag:

    bronze = PythonOperator(task_id="bronze_layer", python_callable=run_bronze)
    silver = PythonOperator(task_id="silver_layer", python_callable=run_silver)
    gold   = PythonOperator(task_id="gold_layer",   python_callable=run_gold)

    bronze >> silver >> gold