import io
import logging
from contextlib import contextmanager
from time import perf_counter
import psycopg2
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import pandas as pd
from etl_pipeline_v2.utils import measure

def build_connection_string(cfg: dict) -> str:
    pg = cfg["postgres"]
    return f"postgresql+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['db']}"

@contextmanager
def postgres_cursor(cfg: dict):
    conn = psycopg2.connect(host=cfg["postgres"]["host"], port=cfg["postgres"]["port"],
        dbname=cfg["postgres"]["db"], user=cfg["postgres"]["user"], password=cfg["postgres"]["password"])
    cur = conn.cursor()
    try:
        yield conn, cur
    finally:
        cur.close(); conn.close()

def create_table(cfg: dict, pqfile: pq.ParquetFile):
    engine = create_engine(build_connection_string(cfg))
    df_schema = pqfile.read_row_group(0).slice(0, 0).to_pandas()
    df_schema.columns = [c.lower() for c in df_schema.columns]
    df_schema.to_sql(cfg["postgres"]["table"], engine, if_exists="replace", index=False)
    engine.dispose()
    return list(df_schema.columns)

def run(cfg: dict, pqfile: pq.ParquetFile, row_groups_iter):
    cols = create_table(cfg, pqfile)
    total_rows, total_groups = 0, 0
    with measure("load"):
        with postgres_cursor(cfg) as (conn, cur):
            for gi, total, df in row_groups_iter:
                buf = io.StringIO()
                df.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cur.copy_expert(f"COPY {cfg['postgres']['table']} FROM STDIN WITH CSV", buf)
                conn.commit()
                total_rows += len(df); total_groups += 1
                logging.getLogger("etl_pipeline").info(f"Row group {gi+1}/{total} | {len(df):,} filas")
    logging.getLogger("etl_pipeline").info(f"Carga finalizada - {total_rows:,} filas en {total_groups} grupos.")
    return {"total_rows": total_rows, "total_groups": total_groups}
