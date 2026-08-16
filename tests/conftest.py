"""Shared pytest fixtures.

`spark` (session-scoped): one SparkSession per pytest run. Spark startup is
slow, so amortize it. The session is minimal — no Hive, no metastore,
no warehouse — because the planner/validator now read Parquet files off
the filesystem via `spark.read.parquet(path)`.
"""

from __future__ import annotations

import pytest

from causalops.spark_session import build_local_spark_session


@pytest.fixture(scope="session")
def spark():
    spark = build_local_spark_session(app_name="causalops_tests")
    yield spark
    spark.stop()
