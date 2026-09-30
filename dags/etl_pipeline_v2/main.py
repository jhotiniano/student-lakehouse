"""
main.py - Orquestador del pipeline ETL

Soporta ejecucion completa (todas las etapas) o granular
por etapa(s) para permitir re-ejecucion independiente desde Airflow.
"""
import io
import logging
import sys
from time import perf_counter

import pandas as pd

from etl_pipeline_v2.utils import load_config, setup_logging
from etl_pipeline_v2.bronze import extract
from etl_pipeline_v2.silver import transform_silver, load_silver
from etl_pipeline_v2.gold import transform_gold, load_gold

logger = setup_logging()

PANDAS_DTYPE_BACKEND = "numpy_nullable"

_global_lookup_bytes = None


def run_pipeline(cfg=None, config_path="config.yml", stages=None):
    """
    Ejecuta el pipeline ETL completo o por etapas.

    Args:
        cfg:          Config dict ya cargado. Si es None, se carga desde config_path.
        config_path:  Ruta al config.yml (usado si cfg es None).
        stages:       Lista de etapas a ejecutar. Si es None, ejecuta todas.
                      Opciones: ["extract", "silver", "gold"]
    """
    global _global_lookup_bytes

    if isinstance(cfg, str):
        config_path, cfg = cfg, None

    t_global = perf_counter()
    stages = stages or ["extract", "silver", "gold"]

    logger.info("=" * 60)
    logger.info(f"Pipeline ETL iniciado - etapas: {stages}")
    logger.info("=" * 60)

    if cfg is None:
        try:
            cfg = load_config(config_path)
            logger.info("Configuracion cargada correctamente.")
        except (FileNotFoundError, EnvironmentError) as e:
            logger.error(f"Error de configuracion: {e}")
            sys.exit(1)

    s3 = extract.build_s3_client(cfg)

    if "extract" in stages:
        logger.info("-- ETAPA 1: EXTRACT BRONZE - Viajes --")
        try:
            extract.run(cfg)
        except Exception as e:
            logger.error(f"Fallo en EXTRACT (viajes): {e}", exc_info=True)
            sys.exit(1)

        logger.info("-- ETAPA 2: EXTRACT BRONZE - Lookup --")
        try:
            _global_lookup_bytes = extract.run_lookup(cfg, s3=s3)
        except Exception as e:
            logger.error(f"Fallo en EXTRACT (lookup): {e}", exc_info=True)
            sys.exit(1)

    if "silver" in stages:
        logger.info("-- ETAPA 3: TRANSFORM + LOAD SILVER --")
        try:
            df_bronze = load_silver.read_bronze(s3, cfg)
            df_silver = transform_silver.run(df_bronze)
            silver_metrics = load_silver.run(cfg, s3, df_silver)
            logger.info(f"Silver completado - {silver_metrics['total_rows']:,} filas")
        except Exception as e:
            logger.error(f"Fallo en SILVER: {e}", exc_info=True)
            sys.exit(1)

    if "gold" in stages:
        logger.info("-- ETAPA 4: TRANSFORM + LOAD GOLD --")
        try:
            if _global_lookup_bytes is None:
                obj = s3.get_object(Bucket=cfg["lookup"]["bucket"], Key=cfg["lookup"]["object_path"])
                df_zones = pd.read_csv(io.BytesIO(obj["Body"].read()))
            else:
                df_zones = pd.read_csv(io.BytesIO(_global_lookup_bytes))

            obj = s3.get_object(Bucket=cfg["silver"]["bucket"], Key=cfg["silver"]["object_path"])
            df_silver = pd.read_parquet(io.BytesIO(obj["Body"].read()))

            gold_models = transform_gold.run(df_silver, df_zones)
            gold_metrics = load_gold.run(cfg, s3, gold_models)
        except Exception as e:
            logger.error(f"Fallo en GOLD: {e}", exc_info=True)
            sys.exit(1)

        elapsed_total = perf_counter() - t_global
        logger.info("=" * 60)
        logger.info(f"Gold completado - {len(gold_metrics)} modelos | tiempo={elapsed_total:.2f}s")
        for model_name, m in gold_metrics.items():
            logger.info(f"  {model_name}: {m['rows']:,} filas -> PostgreSQL")
        logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Pipeline - NYC Taxi Lakehouse")
    parser.add_argument("--config", default="config.yml")
    args = parser.parse_args()
    run_pipeline(config_path=args.config)
