"""
DAG: dag_etl_pipeline_v2.py
Ejecuta el pipeline modular (FASE-3) dentro de Apache Airflow.
"""
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAG_FOLDER not in sys.path:
    sys.path.insert(0, DAG_FOLDER)

from etl_pipeline_v2.main import run_pipeline


def ejecutar_pipeline() -> None:
    config_path = os.path.join(DAG_FOLDER, "etl_pipeline_v2", "config.yml")
    run_pipeline(config_path=config_path)


with DAG(
    dag_id="etl_pipeline_v2",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fase4", "nyc_taxi"],
    default_args={"owner": "data_team"},
    description="Pipeline modular NYC Taxi Bronze -> Silver -> Gold",
) as dag:
    run_etl_pipeline = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=ejecutar_pipeline,
    )
