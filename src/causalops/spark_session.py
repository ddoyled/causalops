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
import shlex
import sys
from pathlib import Path

from pyspark.sql import SparkSession

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
    # Ambient SPARK_HOME (e.g. from a dev-box dotfile) can point at a different
    # Spark install and load the wrong jars into the pip-installed distribution.
    if master.startswith("local"):
        os.environ.pop("SPARK_HOME", None)
        # Pin worker Python to the driver's interpreter — without this, workers
        # may pick a system Python that mismatches the driver's venv and fail
        # with a PYSPARK_PYTHON driver/worker version-mismatch error.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        # On WSL2 the hostname resolves to 127.0.1.1, which Spark refuses as a
        # bind address and warns loudly about. Pin loopback for local mode.
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        # Point log4j2 at our config before the JVM boots — Spark's default
        # falls back to log4j-defaults.properties, which prints a "Setting
        # default log level to WARN" banner and lets the NativeCodeLoader
        # warning through. Configuring here means those never fire.
        # PYSPARK_SUBMIT_ARGS is only read on cold JVM start; if a session
        # already exists in this process, the earlier config wins.
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
        # Small local session — reduce shuffle partitions to keep tests fast.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Silence the in-place "[Stage 0:> ...]" progress bar on CLI runs.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    # Suppress Log4j's boot-time INFO line and the harmless
    # "Unable to load native-hadoop library" warning. Errors still surface.
    spark.sparkContext.setLogLevel("ERROR")
    return spark
