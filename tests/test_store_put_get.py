"""Tests for SparkHiveSpecStore.put / exists / get."""

import pytest

from causalops.store.base import Status
from causalops.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _make_store(spark, db):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    return store


def _put(store, spec):
    return store.put(
        spec,
        git_repo="org/uplift-model",
        git_tag=f"v{spec.version}",
        git_sha="a" * 40,
        registered_by="alice",
        sdk_version="0.1.0",
    )


def test_put_inserts_registration_and_initial_experiment_event(spark, registry_db):
    store = _make_store(spark, registry_db)
    spec = sample_spec()
    reg = _put(store, spec)
    assert reg.family == "uplift" and reg.version == "3.1.0"

    reg_rows = spark.table(f"{registry_db}.registrations").collect()
    assert len(reg_rows) == 1
    assert reg_rows[0]["family"] == "uplift"

    status_rows = spark.table(f"{registry_db}.status_log").collect()
    assert len(status_rows) == 1
    assert status_rows[0]["status"] == Status.EXPERIMENT.value


def test_put_rejects_duplicate_family_version(spark, registry_db):
    store = _make_store(spark, registry_db)
    spec = sample_spec()
    _put(store, spec)
    with pytest.raises(KeyError, match="already registered"):
        _put(store, spec)


def test_exists_and_get(spark, registry_db):
    store = _make_store(spark, registry_db)
    spec = sample_spec()
    assert not store.exists("uplift", "3.1.0")
    _put(store, spec)
    assert store.exists("uplift", "3.1.0")
    fetched = store.get("uplift", "3.1.0")
    assert fetched.spec == spec
    assert fetched.git_repo == "org/uplift-model"
    assert fetched.git_sha == "a" * 40


def test_get_raises_when_missing(spark, registry_db):
    store = _make_store(spark, registry_db)
    with pytest.raises(KeyError, match="not found"):
        store.get("uplift", "9.9.9")
