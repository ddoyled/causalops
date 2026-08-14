"""Tests for the Spark session factory used by the local store.

The `spark` fixture in conftest.py is built via `build_local_spark_session`,
so the fixture path IS the factory path — this test exercises it by hitting
Delta + Hive through the shared session, without standing up (or tearing
down) its own JVM session (which would kill the shared one).
"""
from pyspark.sql import functions as F


def test_local_spark_session_supports_delta_and_hive(spark, registry_db):
    (
        spark.createDataFrame([(1,), (2,)], schema="x INT")
        .write.format("delta")
        .saveAsTable(f"{registry_db}.tt")
    )
    total = spark.table(f"{registry_db}.tt").agg(F.sum("x").alias("s")).collect()[0]["s"]
    assert total == 3
