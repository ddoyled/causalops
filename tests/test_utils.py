"""Tests for the Spark session factory used by the planner.

The `spark` fixture in conftest.py is built via `build_local_spark_session`,
so the fixture path IS the factory path — this test exercises Parquet round-
tripping through the shared session (without standing up its own JVM session,
which would kill the shared one).
"""

from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F


def test_local_spark_session_reads_parquet(spark, tmp_path: Path):
    path = tmp_path / "t.parquet"
    pd.DataFrame({"x": [1, 2]}).to_parquet(path, engine="pyarrow", index=False)
    total = spark.read.parquet(str(path)).agg(F.sum("x").alias("s")).collect()[0]["s"]
    assert total == 3
