import os
import time
import logging
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional
import yaml

def setup_logging(level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("etl_pipeline")

logger = setup_logging()

@dataclass
class StageMetrics:
    stage: str
    elapsed_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    rows_processed: int = 0
    extra: dict = field(default_factory=dict)
    def log(self):
        logger.info(
            f"[METRICS] stage={self.stage} | "
            f"time={self.elapsed_seconds:.2f}s | "
            f"peak_mem={self.peak_memory_mb:.1f}MB | "
            f"rows={self.rows_processed:,}"
            + (f" | {self.extra}" if self.extra else "")
        )

@contextmanager
def measure(stage: str, rows: int = 0, extra: Optional[dict] = None, track_memory: bool = False):
    if track_memory:
        tracemalloc.start()
    t_start = time.perf_counter()
    metrics = StageMetrics(stage=stage, rows_processed=rows, extra=extra or {})
    yield metrics
    elapsed = time.perf_counter() - t_start
    metrics.elapsed_seconds = elapsed
    if track_memory:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        metrics.peak_memory_mb = peak / (1024 ** 2)
    metrics.rows_processed = rows
    metrics.log()

def load_config(path: str = "config.yml") -> dict:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["minio"]["access_key"] = _require_env("MINIO_ROOT_USER")
    cfg["minio"]["secret_key"] = _require_env("MINIO_ROOT_PASSWORD")
    cfg["postgres"]["password"] = _require_env("POSTGRES_PASSWORD")
    return cfg

def _require_env(var: str) -> str:
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(f"Variable requerida no encontrada: '{var}'")
    return value
