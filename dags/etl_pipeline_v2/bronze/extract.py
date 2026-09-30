import io, logging
import boto3, requests
import pyarrow.parquet as pq
from etl_pipeline_v2.utils import measure

def build_s3_client(cfg: dict):
    return boto3.client("s3", endpoint_url=cfg["minio"]["endpoint"],
        aws_access_key_id=cfg["minio"]["access_key"],
        aws_secret_access_key=cfg["minio"]["secret_key"])

def ensure_bucket(s3, bucket: str):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

def already_in_bronze(s3, bucket, object_path):
    try:
        s3.head_object(Bucket=bucket, Key=object_path)
        return True
    except s3.exceptions.ClientError:
        return False

def run(cfg: dict):
    s3 = build_s3_client(cfg)
    bucket, obj_path = cfg["bronze"]["bucket"], cfg["bronze"]["object_path"]
    ensure_bucket(s3, bucket)
    with measure("extract"):
        if not already_in_bronze(s3, bucket, obj_path):
            r = requests.get(cfg["source"]["url"], timeout=120)
            r.raise_for_status()
            s3.put_object(Bucket=bucket, Key=obj_path, Body=r.content)
        obj = s3.get_object(Bucket=bucket, Key=obj_path)
        pqfile = pq.ParquetFile(io.BytesIO(obj["Body"].read()))
    return pqfile

def run_lookup(cfg: dict, s3=None):
    import requests
    s3 = s3 or build_s3_client(cfg)
    bucket = cfg["lookup"]["bucket"]
    obj_path = cfg["lookup"]["object_path"]
    ensure_bucket(s3, bucket)
    try:
        s3.head_object(Bucket=bucket, Key=obj_path)
        print(f"✔ Lookup ya en Bronze.")
    except s3.exceptions.ClientError:
        r = requests.get(cfg["lookup"]["url"], timeout=120)
        r.raise_for_status()
        s3.put_object(Bucket=bucket, Key=obj_path, Body=r.content)
        print(f"✔ Lookup guardado en {bucket}/{obj_path}")
    obj = s3.get_object(Bucket=bucket, Key=obj_path)
    return obj["Body"].read()
