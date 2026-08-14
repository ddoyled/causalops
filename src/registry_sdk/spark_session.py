"""Local Spark session factory (Delta + embedded-Derby Hive metastore).

Mirrors the Spark 3.5 surface used in DBR 16.4 so code written against this
session is portable to a Databricks cluster with minimal changes.
"""
from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_local_spark_session(
    *,
    warehouse_dir: Path | str,
    metastore_dir: Path | str,
    app_name: str = "registry_sdk",
    master: str = "local[2]",
) -> SparkSession:
    """Build (or reuse) a local Spark session with Delta + Hive support.

    Each tmp warehouse/metastore pair gives an isolated catalog, which is what
    the per-test `store` fixture wants. Because Spark only allows one active
    session per JVM, callers that need real isolation should reuse the
    session-scoped fixture and just create fresh databases inside it.
    """
    # Ambient SPARK_HOME (e.g. from a dev-box dotfile) can point at a different
    # Spark install and load the wrong jars into the pip-installed distribution.
    # For local sessions we always want the jars pip installed alongside pyspark.
    if master.startswith("local"):
        import os
        os.environ.pop("SPARK_HOME", None)

    warehouse_dir = Path(warehouse_dir)
    metastore_dir = Path(metastore_dir)
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    metastore_dir.parent.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Small local session — reduce shuffle partitions to keep tests fast.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
