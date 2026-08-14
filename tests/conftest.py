"""Shared pytest fixtures.

- `spark` (session-scoped): one SparkSession per pytest run. Spark startup is
  slow, so amortize it.
- `registry_db` (function-scoped): a unique database name per test, dropped
  on teardown, so store tests get a clean namespace without restarting Spark.
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from registry_sdk.spark_session import build_local_spark_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("spark")
    spark = build_local_spark_session(
        warehouse_dir=root / "warehouse",
        metastore_dir=root / "metastore_db",
        app_name="registry_sdk_tests",
    )
    yield spark
    spark.stop()


@pytest.fixture
def registry_db(spark):
    db_name = f"reg_{uuid.uuid4().hex[:12]}"
    spark.sql(f"CREATE DATABASE {db_name}")
    yield db_name
    spark.sql(f"DROP DATABASE {db_name} CASCADE")
