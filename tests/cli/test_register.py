"""Tests for the `causalops register` CLI."""

import textwrap
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from causalops.cli import cli
from causalops.store.json_file import JsonFileSpecStore


def _write_spec(tmp_path, family="uplift", version="3.1.0", table_path=None):
    """Emit a model_spec.py file that defines a top-level `spec` variable."""
    table_path = table_path or str(tmp_path / "nonexistent.parquet")
    spec_py = tmp_path / "model_spec.py"
    spec_py.write_text(
        textwrap.dedent(f"""
        from causalops import Metric, ModelSpec, Table
        spec = ModelSpec(
            family="{family}", version="{version}", measurement_key="k",
            tables=[Table(name="t", path=r"{table_path}", key="k",
                          metrics=[Metric(name="ate", column="ate", dtype="double")])],
        )
    """)
    )
    return spec_py


def _write_parquet(path: Path, columns: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf = pd.DataFrame({name: pd.Series(dtype=dt) for name, dt in columns.items()})
    pdf.to_parquet(path, engine="pyarrow", index=False)


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


def test_register_happy_path_writes_registration(spark, tmp_path):
    store = JsonFileSpecStore(path=tmp_path / "registry.json")
    spec_py = _write_spec(tmp_path)
    result = _invoke_register(spark, store, spec_py)
    assert result.exit_code == 0, result.output
    assert store.exists("uplift", "3.1.0")


def test_register_rejects_git_tag_mismatch(spark, tmp_path):
    store = JsonFileSpecStore(path=tmp_path / "registry.json")
    spec_py = _write_spec(tmp_path, version="3.2.0")  # spec says 3.2.0
    result = _invoke_register(spark, store, spec_py)  # CLI passes tag v3.1.0
    assert result.exit_code != 0
    assert "does not match spec version" in result.output


def test_register_is_idempotent_without_force(spark, tmp_path):
    store = JsonFileSpecStore(path=tmp_path / "registry.json")
    spec_py = _write_spec(tmp_path)
    r1 = _invoke_register(spark, store, spec_py)
    assert r1.exit_code == 0
    r2 = _invoke_register(spark, store, spec_py)
    assert r2.exit_code != 0
    assert "already registered" in r2.output


def test_register_fails_on_catalog_drift(spark, tmp_path):
    store = JsonFileSpecStore(path=tmp_path / "registry.json")
    # Parquet with the wrong dtype for 'ate'.
    results = tmp_path / "results.parquet"
    _write_parquet(results, {"k": "string", "ate": "string"})
    spec_py = _write_spec(tmp_path, table_path=str(results))
    result = _invoke_register(spark, store, spec_py)
    assert result.exit_code != 0
    assert "UC validation failed" in result.output


def test_register_force_overwrites(spark, tmp_path):
    store = JsonFileSpecStore(path=tmp_path / "registry.json")
    spec_py = _write_spec(tmp_path)
    assert _invoke_register(spark, store, spec_py).exit_code == 0
    result = _invoke_register(spark, store, spec_py, extra=["--force"])
    assert result.exit_code == 0, result.output
    assert store.exists("uplift", "3.1.0")
