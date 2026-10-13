import logging
import sys
from time import perf_counter

import pyarrow.parquet as pq

from utils import load_config, setup_logging
import extract
import transform
import load


logger = setup_logging()

PANDAS_DTYPE_BACKEND = "numpy_nullable"


def row_groups_pipeline(pqfile: pq.ParquetFile):
    total = pqfile.num_row_groups
    for i in range(total):
        df_raw = pqfile.read_row_group(i).to_pandas()
        df_clean = transform.run(df_raw)
        yield i, total, df_clean


def run_pipeline(config_path: str = "config.yml") -> None:
    t_global = perf_counter()
    logger.info("=" * 60)
    logger.info("Pipeline ETL iniciado")
    logger.info("=" * 60)

    # CONFIG
    try:
        cfg = load_config(config_path)
        logger.info("Configuración cargada correctamente.")
    except (FileNotFoundError, EnvironmentError) as e:
        logger.error(f"Error de configuración: {e}")
        sys.exit(1)

    # EXTRACT
    logger.info("--- ETAPA 1: EXTRACT ---")
    try:
        pqfile = extract.run(cfg)
    except Exception as e:
        logger.error(f"Fallo en EXTRACT: {e}", exc_info=True)
        sys.exit(1)

    # TRANSFORM + LOAD
    logger.info("--- ETAPA 2: TRANSFORM + LOAD ---")
    try:
        metrics = load.run(
            cfg=cfg,
            pqfile=pqfile,
            row_groups_iter=row_groups_pipeline(pqfile),
        )
    except Exception as e:
        logger.error(f"Fallo en LOAD: {e}", exc_info=True)
        sys.exit(1)

    # REPORTE FINAL
    elapsed_total = perf_counter() - t_global
    logger.info("=" * 60)
    logger.info(
        f"Pipeline finalizado exitosamente | "
        f"filas={metrics['total_rows']:,} | "
        f"grupos={metrics['total_groups']} | "
        f"tiempo_total={elapsed_total:.2f}s"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Pipeline - NYC Taxi Lakehouse")
    parser.add_argument("--config", default="config.yml")
    args = parser.parse_args()
    run_pipeline(config_path=args.config)