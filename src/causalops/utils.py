"""Spark utilities: session factory and table reader.

``build_local_spark_session`` creates a minimal local SparkSession for reading
Parquet — just enough for the planner and validator. Mirrors the Spark 3.5
surface used in DBR 16.4 so planner code is portable to Databricks clusters.

``read_table`` interprets ``Table.path`` values two ways:

- Filesystem paths (contain ``/`` or a URL scheme) go through
  ``spark.read.parquet(path)``. Local dev uses this.
- Anything else is treated as a catalog identifier and read via
  ``spark.table(path)``. This is the Databricks / Unity Catalog path.
"""

from __future__ import annotations

import os
import shlex
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_LOG4J_CONFIG = Path(__file__).parent / "log4j2.properties"


def build_local_spark_session(
    *,
    app_name: str = "causalops",
    master: str = "local[2]",
) -> SparkSession:
    """Build (or reuse) a local Spark session for reading Parquet.

    Spark only allows one active session per JVM, so callers that need
    isolation should share the session-scoped pytest fixture rather than
    building fresh ones.
    """
    from pyspark.sql import SparkSession

    if master.startswith("local"):
        os.environ.pop("SPARK_HOME", None)
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault(
            "PYSPARK_SUBMIT_ARGS",
            shlex.join(
                [
                    "--driver-java-options",
                    f"-Dlog4j2.configurationFile={_LOG4J_CONFIG}",
                    "pyspark-shell",
                ]
            ),
        )

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _looks_like_filesystem_path(path: str) -> bool:
    return "/" in path or "://" in path


def read_table(spark: SparkSession, path: str) -> DataFrame:
    """Read a spec-declared table as a DataFrame (Parquet path or catalog name)."""
    if _looks_like_filesystem_path(path):
        return spark.read.parquet(path)
    return spark.table(path)
