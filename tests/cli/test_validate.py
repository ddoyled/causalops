"""Tests for the `causalops validate` CLI — dry-run schema check without registering."""

import textwrap
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from causalops.cli import cli


def _write_spec(tmp_path: Path, table_path: str) -> Path:
    spec_py = tmp_path / "model_spec.py"
    spec_py.write_text(
        textwrap.dedent(f"""
        from causalops import Metric, ModelSpec, Table
        spec = ModelSpec(
            family="uplift", version="3.1.0", measurement_key="k",
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


def _invoke(spark, spec_py: Path):
    return CliRunner().invoke(cli, ["validate", "--spec-path", str(spec_py)], obj={"spark": spark})


def test_validate_matching_table_exits_zero(spark, tmp_path):
    parquet = tmp_path / "results.parquet"
    _write_parquet(parquet, {"k": "string", "ate": "float64"})
    result = _invoke(spark, _write_spec(tmp_path, str(parquet)))
    assert result.exit_code == 0, result.output
    assert "ok" in result.output.lower()


def test_validate_missing_table_warns_but_exits_zero(spark, tmp_path):
    result = _invoke(spark, _write_spec(tmp_path, str(tmp_path / "missing.parquet")))
    assert result.exit_code == 0, result.output
    assert "does not exist" in result.output


def test_validate_dtype_mismatch_exits_nonzero(spark, tmp_path):
    parquet = tmp_path / "results.parquet"
    _write_parquet(parquet, {"k": "string", "ate": "string"})
    result = _invoke(spark, _write_spec(tmp_path, str(parquet)))
    assert result.exit_code != 0
    joined = result.output
    assert "ate" in joined and "double" in joined and "string" in joined
