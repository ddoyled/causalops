"""Tests for the Spark session factory used by the local store.

The `spark` fixture in conftest.py is built via `build_local_spark_session`,
so the fixture path IS the factory path — this test exercises it by hitting
Delta + Hive through the shared session, without standing up (or tearing
down) its own JVM session (which would kill the shared one).
"""

from pathlib import Path

from pyspark.sql import functions as F


def test_local_spark_session_supports_delta_and_hive(spark, registry_db):
    (
        spark.createDataFrame([(1,), (2,)], schema="x INT")
        .write.format("delta")
        .saveAsTable(f"{registry_db}.tt")
    )
    total = spark.table(f"{registry_db}.tt").agg(F.sum("x").alias("s")).collect()[0]["s"]
    assert total == 3


def test_derby_log_is_redirected_out_of_cwd(spark):
    """The factory pins `derby.stream.error.file` so Derby's log lands next to
    the metastore, not in the JVM's cwd. Regression against derby.log landing
    in the repo root on every pytest / CLI invocation."""
    # Force the Hive metastore (and therefore Derby) to actually boot.
    spark.sql("SHOW DATABASES").collect()

    log_path = spark._jvm.java.lang.System.getProperty("derby.stream.error.file")
    assert log_path, "derby.stream.error.file JVM property was not set"

    p = Path(log_path).resolve()
    assert p.exists(), f"expected Derby to have written {p}"
    assert p.name == "derby.log"
    assert p.parent != Path.cwd().resolve(), (
        f"derby.log ended up in cwd ({p.parent}); redirection did not take effect"
    )
