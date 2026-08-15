"""Registry storage backends and factory."""
from __future__ import annotations

import json
import os

from causalops.store.base import Registration, SpecStore, Status, StatusEvent
from causalops.store.spark_hive import SparkHiveSpecStore

__all__ = [
    "Registration", "SpecStore", "Status", "StatusEvent",
    "SparkHiveSpecStore", "get_store",
]


def get_store(spark=None) -> SpecStore:
    """Build the configured SpecStore.

    Reads `CAUSALOPS_STORE_CONFIG` (JSON) from the environment, e.g.
        {"backend": "spark_hive", "database": "main.registry"}

    Defaults to `{"backend": "spark_hive", "database": "registry"}`.
    """
    raw = os.environ.get(
        "CAUSALOPS_STORE_CONFIG",
        '{"backend": "spark_hive", "database": "registry"}',
    )
    cfg = json.loads(raw)
    backend = cfg.get("backend", "spark_hive")
    if backend != "spark_hive":
        raise ValueError(f"unsupported backend {backend!r}")
    if spark is None:
        raise ValueError("get_store requires a SparkSession for the spark_hive backend")
    store = SparkHiveSpecStore(spark=spark, database=cfg["database"])
    store.ensure_tables()
    return store
