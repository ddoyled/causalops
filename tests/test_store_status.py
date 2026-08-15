"""Tests for status query methods on SparkHiveSpecStore."""
from datetime import datetime, timedelta, timezone

import pytest

from causalops.store.base import Status
from causalops.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _store_with(spark, db, *versions):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for v in versions:
        store.put(
            sample_spec(version=v),
            git_repo="org/uplift-model", git_tag=f"v{v}",
            git_sha="a" * 40, registered_by="alice", sdk_version="0.1.0",
        )
    return store


def test_list_families_and_versions(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0", "3.1.0")
    assert store.list_families() == ["uplift"]
    assert sorted(store.list_versions("uplift")) == ["3.0.0", "3.1.0"]


def test_current_status_starts_as_experiment(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    assert store.current_status("uplift", "3.0.0") == Status.EXPERIMENT


def test_by_status_returns_matching_registrations(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0", "3.1.0")
    regs = store.by_status("uplift", Status.EXPERIMENT)
    assert sorted(r.version for r in regs) == ["3.0.0", "3.1.0"]
    assert store.by_status("uplift", Status.PRODUCTION) == []


def test_by_status_accepts_list_of_statuses(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    regs = store.by_status("uplift", [Status.EXPERIMENT, Status.CHALLENGER])
    assert len(regs) == 1


def test_history_returns_events_in_order(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    events = store.history("uplift", "3.0.0")
    assert len(events) == 1
    assert events[0].status == Status.EXPERIMENT


def test_current_status_missing_version_raises(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    with pytest.raises(KeyError):
        store.current_status("uplift", "9.9.9")


def test_current_status_respects_as_of(spark, registry_db):
    """Point-in-time query: an event added after `as_of` must not be visible."""
    store = _store_with(spark, registry_db, "3.0.0")
    before = datetime.now(timezone.utc) - timedelta(days=365)
    # future event: simulate by appending directly through helper
    import uuid
    future_row = (
        str(uuid.uuid4()), "uplift", "3.0.0", Status.PRODUCTION.value,
        datetime.now(timezone.utc) + timedelta(days=1), "alice", "",
    )
    from causalops.store.spark_hive import _STATUS_SCHEMA
    spark.createDataFrame([future_row], schema=_STATUS_SCHEMA).write.format(
        "delta"
    ).mode("append").saveAsTable(f"{registry_db}.status_log")

    # as_of before any event -> KeyError
    with pytest.raises(KeyError):
        store.current_status("uplift", "3.0.0", as_of=before)
    # as_of now -> experiment (future event excluded)
    assert store.current_status(
        "uplift", "3.0.0", as_of=datetime.now(timezone.utc),
    ) == Status.EXPERIMENT
