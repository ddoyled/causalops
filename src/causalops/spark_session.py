"""Local Spark session factory.

Just enough Spark to run the planner: an in-process SparkSession that can
read Parquet files off the local filesystem. No Hive metastore, no Derby, no
warehouse directory.

Mirrors the Spark 3.5 surface used in DBR 16.4 so planner code written
against this session is portable to a Databricks cluster with minimal
changes (see README's "Migrating to Databricks" section).
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


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
    # Ambient SPARK_HOME (e.g. from a dev-box dotfile) can point at a different
    # Spark install and load the wrong jars into the pip-installed distribution.
    if master.startswith("local"):
        os.environ.pop("SPARK_HOME", None)
        # Pin worker Python to the driver's interpreter — without this, workers
        # may pick a system Python that mismatches the driver's venv and fail
        # with a PYSPARK_PYTHON driver/worker version-mismatch error.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Small local session — reduce shuffle partitions to keep tests fast.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
