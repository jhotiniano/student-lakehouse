import io, logging
from time import perf_counter
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
from utils import measure
from load import build_connection_string, postgres_cursor
logger = logging.getLogger("etl_pipeline.gold.load")

def ensure_bucket(s3, bucket: str):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

def run(cfg: dict, s3, models: dict) -> dict:
    ensure_bucket(s3, cfg["gold"]["bucket"])
    metrics = {}
    with measure("load_gold"):
        for name, df in models.items():
            mcfg = cfg["gold"]["models"][name]
            table = pa.Table.from_pandas(df, preserve_index=False)
            buf = io.BytesIO()
            pq.write_table(table, buf); buf.seek(0)
            s3.put_object(Bucket=cfg["gold"]["bucket"], Key=mcfg["object_path"], Body=buf.read())
            engine = create_engine(build_connection_string(cfg))
            df.iloc[0:0].to_sql(mcfg["table"], engine, if_exists="replace", index=False)
            engine.dispose()
            with postgres_cursor(cfg) as (conn, cur):
                cbuf = io.StringIO()
                df.to_csv(cbuf, index=False, header=False); cbuf.seek(0)
                cur.copy_expert(f"COPY {mcfg['table']} FROM STDIN WITH CSV", cbuf)
                conn.commit()
            metrics[name] = {"rows": len(df)}
    return metrics
