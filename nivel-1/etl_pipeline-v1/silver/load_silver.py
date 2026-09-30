import io, logging
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from utils import measure
logger = logging.getLogger("etl_pipeline.silver.load")

def read_bronze(s3, cfg: dict) -> pd.DataFrame:
    obj = s3.get_object(Bucket=cfg["bronze"]["bucket"], Key=cfg["bronze"]["object_path"])
    df = pq.read_table(io.BytesIO(obj["Body"].read())).to_pandas()
    logger.info(f"Bronze leído — {len(df):,} filas")
    return df

def ensure_bucket(s3, bucket: str):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

def write_silver(s3, cfg: dict, df: pd.DataFrame):
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=cfg["silver"]["bucket"], Key=cfg["silver"]["object_path"], Body=buf.read())
    logger.info(f"Silver escrito → {cfg['silver']['bucket']}/{cfg['silver']['object_path']}")

def run(cfg: dict, s3, df_silver: pd.DataFrame) -> dict:
    ensure_bucket(s3, cfg["silver"]["bucket"])
    with measure("load_silver"):
        write_silver(s3, cfg, df_silver)
    return {"total_rows": len(df_silver), "columns": len(df_silver.columns)}
