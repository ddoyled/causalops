"""Tests for SparkHiveSpecStore schema initialization."""
from causalops.store.spark_hive import SparkHiveSpecStore


def test_ensure_tables_creates_both_tables(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    tables = {t.name for t in spark.catalog.listTables(registry_db)}
    assert {"registrations", "status_log"}.issubset(tables)


def test_ensure_tables_is_idempotent(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    store.ensure_tables()  # second call must not raise
    tables = {t.name for t in spark.catalog.listTables(registry_db)}
    assert {"registrations", "status_log"}.issubset(tables)


def test_registrations_schema_matches_spec(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    schema = {
        f.name: f.dataType.simpleString()
        for f in spark.table(f"{registry_db}.registrations").schema.fields
    }
    assert schema == {
        "family": "string",
        "version": "string",
        "spec_json": "string",
        "git_repo": "string",
        "git_tag": "string",
        "git_sha": "string",
        "sdk_version": "string",
        "registered_by": "string",
        "registered_at": "timestamp",
    }


def test_status_log_schema_matches_spec(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    schema = {
        f.name: f.dataType.simpleString()
        for f in spark.table(f"{registry_db}.status_log").schema.fields
    }
    assert schema == {
        "event_id": "string",
        "family": "string",
        "version": "string",
        "status": "string",
        "effective_from": "timestamp",
        "assigned_by": "string",
        "note": "string",
    }
