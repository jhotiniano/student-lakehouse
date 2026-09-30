"""
DAG: etl_fase2_monolith.py
Ejecuta el pipeline monolitico de Fase-2 (carga directa de Parquet a PostgreSQL)
"""
import io
from datetime import datetime
from time import time
import pandas as pd
import psycopg2
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator

PARQUET_FILE = "/opt/airflow/Proyectos/input/yellow_tripdata_2024-01.parquet"
TABLE_NAME = "yellow_taxi_data"
PG_CONFIG = {
    "host": "postgres-container",
    "dbname": "ny_taxi",
    "user": "postgres",
    "password": "changeme1234",
}

def run_etl_fase2():
    engine_url = (
        f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:5432/{PG_CONFIG['dbname']}"
    )
    engine = create_engine(engine_url)
    df_schema = pq.read_table(PARQUET_FILE).slice(0, 0).to_pandas()
    df_schema.to_sql(name=TABLE_NAME, con=engine, if_exists="replace", index=False)
    print("Tabla creada automaticamente (solo esquema)")
    pqfile = pq.ParquetFile(PARQUET_FILE)
    num_groups = pqfile.num_row_groups
    print(f"Parquet detectado con {num_groups} row groups")
    conn = psycopg2.connect(host=PG_CONFIG["host"], dbname=PG_CONFIG["dbname"], user=PG_CONFIG["user"], password=PG_CONFIG["password"])
    cursor = conn.cursor()
    for i in range(num_groups):
        t_start = time()
        print(f"Procesando row group {i + 1}/{num_groups}")
        df = pqfile.read_row_group(i).to_pandas()
        for col in df.columns:
            if df[col].dtype == "float64":
                if ((df[col] % 1 == 0) | df[col].isnull()).all():
                    df[col] = df[col].astype("Int64")
        for col in df.columns:
            if "datetime" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV", buffer)
        conn.commit()
        print(f"Row group {i + 1} cargado en {time() - t_start:.2f} segundos")
    cursor.close()
    conn.close()
    print("Carga completa finalizada")

default_args = {"owner": "data_team", "start_date": datetime(2024, 1, 1), "retries": 1}

with DAG(dag_id="etl_fase2_monolith", default_args=default_args, schedule_interval=None, catchup=False, tags=["fase2", "nyc_taxi"], description="Pipeline monolitico NYC Taxi Fase-2 orquestado con Airflow") as dag:
    tarea_unica = PythonOperator(task_id="cargar_yellow_taxi_data", python_callable=run_etl_fase2)
