"""Tests for the `causalops register` CLI."""

import textwrap

from click.testing import CliRunner

from causalops.cli import cli
from causalops.store.spark_hive import SparkHiveSpecStore


def _write_spec(tmp_path, family="uplift", version="3.1.0", table_path=None):
    """Emit a model_spec.py file that defines a top-level `spec` variable."""
    table_path = table_path or f"nonexistent_ns.{family}.results"
    spec_py = tmp_path / "model_spec.py"
    spec_py.write_text(
        textwrap.dedent(f"""
        from causalops import Metric, ModelSpec, Table
        spec = ModelSpec(
            family="{family}", version="{version}", measurement_key="k",
            tables=[Table(name="t", path="{table_path}", key="k",
                          metrics=[Metric(name="ate", column="ate", dtype="double")])],
        )
    """)
    )
    return spec_py


def _invoke_register(spark, store, spec_py, extra=()):
    """Inject the shared Spark session + store via Click's ctx.obj."""
    runner = CliRunner()
    return runner.invoke(
        cli,
        [
            "register",
            "--spec-path",
            str(spec_py),
            "--git-repo",
            "org/uplift-model",
            "--git-tag",
            "v3.1.0",
            "--git-sha",
            "a" * 40,
            "--registered-by",
            "alice",
            *extra,
        ],
        obj={"spark": spark, "store": store},
    )


def test_register_happy_path_writes_registration(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    spec_py = _write_spec(tmp_path)
    result = _invoke_register(spark, store, spec_py)
    assert result.exit_code == 0, result.output
    assert store.exists("uplift", "3.1.0")


def test_register_rejects_git_tag_mismatch(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    spec_py = _write_spec(tmp_path, version="3.2.0")  # spec says 3.2.0
    result = _invoke_register(spark, store, spec_py)  # CLI passes tag v3.1.0
    assert result.exit_code != 0
    assert "does not match spec version" in result.output


def test_register_is_idempotent_without_force(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    spec_py = _write_spec(tmp_path)
    r1 = _invoke_register(spark, store, spec_py)
    assert r1.exit_code == 0
    r2 = _invoke_register(spark, store, spec_py)
    assert r2.exit_code != 0
    assert "already registered" in r2.output


def test_register_fails_on_catalog_drift(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    # Create a table with the wrong dtype for 'ate'.
    spark.createDataFrame([], "k STRING, ate STRING").write.format("delta").saveAsTable(
        f"{registry_db}.results"
    )
    spec_py = _write_spec(tmp_path, table_path=f"{registry_db}.results")
    result = _invoke_register(spark, store, spec_py)
    assert result.exit_code != 0
    assert "UC validation failed" in result.output
