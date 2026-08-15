"""Tests for RegistryClient (consumer-facing API)."""

import pytest

from causalops import Metric, ModelSpec, RegistryClient, Table
from causalops.store.base import Status
from causalops.store.spark_hive import SparkHiveSpecStore


def _seed_result_tables(spark, db):
    spark.createDataFrame(
        [("e1", 0.10), ("e2", 0.20)],
        schema="experiment_id STRING, ate DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v3")
    spark.createDataFrame(
        [("e1", 0.09), ("e2", 0.19)],
        schema="experiment_id STRING, te DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v2")


def _spec(family, version, db, table_suffix, phys):
    return ModelSpec(
        family=family,
        version=version,
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path=f"{db}.shared_{table_suffix}",
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column=phys, dtype="double", aliases=["te"]),
                ],
            )
        ],
    )


def _seed_registry(spark, db):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for spec in [
        _spec("uplift", "2.9.0", db, "v2", "te"),
        _spec("uplift", "3.0.0", db, "v3", "ate"),
    ]:
        store.put(
            spec,
            git_repo="org/uplift-model",
            git_tag=f"v{spec.version}",
            git_sha="a" * 40,
            registered_by="alice",
            sdk_version="0.1.0",
        )
    return store


def test_get_results_by_explicit_version(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    store = _seed_registry(spark, registry_db)
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(family="uplift", version="3.0.0", metrics=["treatment_effect"])
    assert {"experiment_id", "treatment_effect"} <= set(df.columns)
    rows = {r["experiment_id"]: r["treatment_effect"] for r in df.collect()}
    assert rows == {"e1": 0.10, "e2": 0.20}


def test_get_results_by_status_production_returns_one_version(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    store = _seed_registry(spark, registry_db)
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(family="uplift", status="production", metrics=["treatment_effect"])
    rows = df.collect()
    assert all(r["version"] == "3.0.0" for r in rows)


def test_get_results_multi_status_unions(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    store = _seed_registry(spark, registry_db)
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "2.9.0", Status.CHALLENGER, assigned_by="bob")
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(
        family="uplift",
        status=["production", "challenger"],
        metrics=["treatment_effect"],
    )
    versions = {r["version"] for r in df.collect()}
    assert versions == {"3.0.0", "2.9.0"}


def test_get_results_rejects_status_and_version_together(spark, registry_db):
    store = _seed_registry(spark, registry_db)
    client = RegistryClient(store=store, spark=spark)
    with pytest.raises(ValueError, match="either"):
        client.get_results(
            family="uplift", version="3.0.0", status="production", metrics=["treatment_effect"]
        )


def test_discovery_wrappers(spark, registry_db):
    store = _seed_registry(spark, registry_db)
    client = RegistryClient(store=store, spark=spark)
    assert client.list_families() == ["uplift"]
    assert sorted(client.list_versions("uplift")) == ["2.9.0", "3.0.0"]
    described = client.describe("uplift", "3.0.0")
    assert described.spec.version == "3.0.0"
    hist = client.history("uplift", "3.0.0")
    assert len(hist) == 1 and hist[0].status == Status.EXPERIMENT
