import io
import logging

import boto3
import requests
import pyarrow.parquet as pq

from utils import measure


def build_s3_client(cfg: dict):
    return boto3.client(
        "s3",
        endpoint_url=cfg["minio"]["endpoint"],
        aws_access_key_id=cfg["minio"]["access_key"],
        aws_secret_access_key=cfg["minio"]["secret_key"],
    )


def ensure_bucket(s3, bucket: str) -> None:
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logging.getLogger("etl_pipeline").info(f"Bucket '{bucket}' creado.")
    else:
        logging.getLogger("etl_pipeline").info(f"Bucket '{bucket}' ya existe.")


def upload_to_bronze(s3, bucket: str, object_path: str, data: bytes) -> None:
    s3.put_object(Bucket=bucket, Key=object_path, Body=data)
    logging.getLogger("etl_pipeline").info(f"Archivo almacenado en Bronze -> {bucket}/{object_path}")


def already_in_bronze(s3, bucket: str, object_path: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=object_path)
        return True
    except s3.exceptions.ClientError:
        return False


def download_parquet(url: str) -> bytes:
    logger = logging.getLogger("etl_pipeline")
    logger.info(f"Descargando desde: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    logger.info(f"Descarga completa - {len(response.content) / (1024**2):.1f} MB")
    return response.content


def run(cfg: dict):
    s3 = build_s3_client(cfg)
    bucket = cfg["bronze"]["bucket"]
    obj_path = cfg["bronze"]["object_path"]

    ensure_bucket(s3, bucket)

    with measure("extract") as m:
        if already_in_bronze(s3, bucket, obj_path):
            logging.getLogger("etl_pipeline").info("Archivo ya en Bronze. Saltando descarga.")
        else:
            raw_bytes = download_parquet(cfg["source"]["url"])
            upload_to_bronze(s3, bucket, obj_path, raw_bytes)

        obj = s3.get_object(Bucket=bucket, Key=obj_path)
        parquet_bytes = io.BytesIO(obj["Body"].read())
        pqfile = pq.ParquetFile(parquet_bytes)

    m.extra = {"num_row_groups": pqfile.num_row_groups}
    logging.getLogger("etl_pipeline").info(f"ParquetFile listo - {pqfile.num_row_groups} row groups")

    return pqfile