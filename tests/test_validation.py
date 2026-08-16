"""Tests for validate_against_uc — must warn on missing tables and error on drift."""

from pathlib import Path

import pandas as pd

from causalops import Metric, ModelSpec, Table
from causalops.validation import validate_against_uc


def _spec(path: str) -> ModelSpec:
    return ModelSpec(
        family="uplift",
        version="3.1.0",
        measurement_key="k",
        tables=[
            Table(
                name="t",
                path=path,
                key="k",
                metrics=[
                    Metric(name="ate", column="ate", dtype="double"),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                ],
            )
        ],
    )


def _write_empty_parquet(path: Path, columns: dict[str, str]) -> None:
    """Write an empty parquet with columns of the given pandas dtypes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf = pd.DataFrame({name: pd.Series(dtype=dt) for name, dt in columns.items()})
    pdf.to_parquet(path, engine="pyarrow", index=False)


def test_missing_table_produces_warning_not_error(spark, tmp_path):
    spec = _spec(str(tmp_path / "does_not_exist.parquet"))
    report = validate_against_uc(spec, spark=spark)
    assert not report.has_errors
    assert report.has_warnings
    assert "does not exist" in "\n".join(report.warnings)


def test_matching_table_produces_no_findings(spark, tmp_path):
    path = tmp_path / "results.parquet"
    _write_empty_parquet(path, {"k": "string", "ate": "float64", "ci_lo": "float64"})
    report = validate_against_uc(_spec(str(path)), spark=spark)
    assert not report.has_errors and not report.has_warnings


def test_missing_column_produces_error(spark, tmp_path):
    path = tmp_path / "results.parquet"
    _write_empty_parquet(path, {"k": "string", "ate": "float64"})
    report = validate_against_uc(_spec(str(path)), spark=spark)
    assert report.has_errors
    assert any("ci_lo" in e for e in report.errors)


def test_dtype_mismatch_produces_error(spark, tmp_path):
    path = tmp_path / "results.parquet"
    _write_empty_parquet(path, {"k": "string", "ate": "string", "ci_lo": "float64"})
    report = validate_against_uc(_spec(str(path)), spark=spark)
    assert report.has_errors
    joined = "\n".join(report.errors)
    assert "ate" in joined and "double" in joined and "string" in joined
