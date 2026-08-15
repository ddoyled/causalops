"""Tests for the `causalops promote` CLI."""

from click.testing import CliRunner

from causalops.cli import cli
from causalops.store.base import Status
from causalops.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _seed(spark, db, *versions):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for v in versions:
        store.put(
            sample_spec(version=v),
            git_repo="org/uplift-model",
            git_tag=f"v{v}",
            git_sha="a" * 40,
            registered_by="alice",
            sdk_version="0.1.0",
        )
    return store


def _invoke_promote(spark, store, args):
    runner = CliRunner()
    return runner.invoke(
        cli,
        ["promote", *args],
        obj={"spark": spark, "store": store},
    )


def test_promote_to_challenger(spark, registry_db):
    store = _seed(spark, registry_db, "3.0.0")
    result = _invoke_promote(
        spark,
        store,
        [
            "--family",
            "uplift",
            "--version",
            "3.0.0",
            "--status",
            "challenger",
            "--assigned-by",
            "bob",
        ],
    )
    assert result.exit_code == 0, result.output
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER


def test_promote_to_production_atomically_retires_prior(spark, registry_db):
    store = _seed(spark, registry_db, "3.0.0", "3.1.0")
    _invoke_promote(
        spark,
        store,
        [
            "--family",
            "uplift",
            "--version",
            "3.0.0",
            "--status",
            "production",
            "--assigned-by",
            "bob",
        ],
    )
    result = _invoke_promote(
        spark,
        store,
        [
            "--family",
            "uplift",
            "--version",
            "3.1.0",
            "--status",
            "production",
            "--assigned-by",
            "bob",
            "--note",
            "graduated",
        ],
    )
    assert result.exit_code == 0, result.output
    prods = [r.version for r in store.by_status("uplift", Status.PRODUCTION)]
    assert prods == ["3.1.0"]


def test_promote_from_retired_requires_reactivate_flag(spark, registry_db):
    store = _seed(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.RETIRED, assigned_by="bob")
    result = _invoke_promote(
        spark,
        store,
        [
            "--family",
            "uplift",
            "--version",
            "3.0.0",
            "--status",
            "challenger",
            "--assigned-by",
            "bob",
        ],
    )
    assert result.exit_code != 0
    assert "reactivate" in result.output
    ok = _invoke_promote(
        spark,
        store,
        [
            "--family",
            "uplift",
            "--version",
            "3.0.0",
            "--status",
            "challenger",
            "--assigned-by",
            "bob",
            "--reactivate",
        ],
    )
    assert ok.exit_code == 0
