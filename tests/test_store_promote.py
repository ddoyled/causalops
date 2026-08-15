"""Tests for promote() including atomic production swap and reactivate guard."""
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


def test_promote_to_challenger(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob")
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER


def test_promote_unknown_version_raises(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    with pytest.raises(KeyError, match="not registered"):
        store.promote("uplift", "9.9.9", Status.CHALLENGER, assigned_by="bob")


def test_promote_to_production_when_none_current(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    prods = store.by_status("uplift", Status.PRODUCTION)
    assert [r.version for r in prods] == ["3.0.0"]


def test_promote_to_production_retires_current_prod_atomically(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0", "3.1.0")
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote(
        "uplift", "3.1.0", Status.PRODUCTION, assigned_by="bob",
        note="passed 2-week challenger review",
    )
    prods = store.by_status("uplift", Status.PRODUCTION)
    assert [r.version for r in prods] == ["3.1.0"]
    assert store.current_status("uplift", "3.0.0") == Status.RETIRED
    # And the retirement was written in the same batch (same effective_from as promote).
    events_30 = [e for e in store.history("uplift", "3.0.0")
                 if e.status == Status.RETIRED]
    events_31 = [e for e in store.history("uplift", "3.1.0")
                 if e.status == Status.PRODUCTION]
    assert events_30 and events_31
    assert events_30[0].effective_from == events_31[0].effective_from


def test_unretire_requires_reactivate_flag(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "3.0.0", Status.RETIRED, assigned_by="bob")
    with pytest.raises(ValueError, match="reactivate"):
        store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob")
    # With flag, it proceeds.
    store.promote(
        "uplift", "3.0.0", Status.CHALLENGER,
        assigned_by="bob", reactivate=True,
    )
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER
