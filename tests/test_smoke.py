"""Smoke test: verify PySpark + Delta start locally with Hive support."""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def test_spark_delta_hive_starts(tmp_path):
    warehouse = tmp_path / "warehouse"
    metastore = tmp_path / "metastore_db"
    builder = (
        SparkSession.builder
        .appName("smoke")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS smoke_db")
        spark.sql(
            "CREATE TABLE smoke_db.t (x INT) USING DELTA"
        )
        spark.sql("INSERT INTO smoke_db.t VALUES (1), (2)")
        rows = spark.sql("SELECT sum(x) AS s FROM smoke_db.t").collect()
        assert rows[0]["s"] == 3
    finally:
        spark.stop()
