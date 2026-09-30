import io, logging, sys
from time import perf_counter
import pandas as pd
from utils import load_config, setup_logging
import extract
from silver import transform_silver, load_silver
from gold import transform_gold, load_gold
logger = setup_logging()

def run_pipeline(config_path="config.yml"):
    t0 = perf_counter()
    cfg = load_config(config_path)
    s3 = extract.build_s3_client(cfg)
    pqfile = extract.run(cfg)
    lookup_bytes = extract.run_lookup(cfg, s3)
    df_bronze = load_silver.read_bronze(s3, cfg)
    df_silver = transform_silver.run(df_bronze)
    load_silver.run(cfg, s3, df_silver)
    df_zones = pd.read_csv(io.BytesIO(lookup_bytes))
    models = transform_gold.run(df_silver, df_zones)
    load_gold.run(cfg, s3, models)
    logger.info(f"Pipeline Fase-3 OK en {perf_counter()-t0:.1f}s")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(); p.add_argument("--config", default="config.yml")
    run_pipeline(p.parse_args().config)
