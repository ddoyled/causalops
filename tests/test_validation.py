"""Tests for validate_against_uc — must warn on missing tables and error on drift."""
from causalops import Metric, ModelSpec, Table
from causalops.validation import validate_against_uc


def _spec(path: str) -> ModelSpec:
    return ModelSpec(
        family="uplift", version="3.1.0", measurement_key="k",
        tables=[Table(name="t", path=path, key="k", metrics=[
            Metric(name="ate", column="ate", dtype="double"),
            Metric(name="ci_lower", column="ci_lo", dtype="double"),
        ])],
    )


def test_missing_table_produces_warning_not_error(spark, registry_db):
    spec = _spec(f"{registry_db}.does_not_exist")
    report = validate_against_uc(spec, spark=spark)
    assert not report.has_errors
    assert report.has_warnings
    assert "does not exist" in "\n".join(report.warnings)


def _write_empty(spark, name, schema):
    """Create an empty Delta table with the given schema (test helper)."""
    spark.createDataFrame([], schema=schema).write.format("delta").saveAsTable(name)


def test_matching_table_produces_no_findings(spark, registry_db):
    _write_empty(spark, f"{registry_db}.results", "k STRING, ate DOUBLE, ci_lo DOUBLE")
    report = validate_against_uc(_spec(f"{registry_db}.results"), spark=spark)
    assert not report.has_errors and not report.has_warnings


def test_missing_column_produces_error(spark, registry_db):
    _write_empty(spark, f"{registry_db}.results", "k STRING, ate DOUBLE")
    report = validate_against_uc(_spec(f"{registry_db}.results"), spark=spark)
    assert report.has_errors
    assert any("ci_lo" in e for e in report.errors)


def test_dtype_mismatch_produces_error(spark, registry_db):
    _write_empty(spark, f"{registry_db}.results", "k STRING, ate STRING, ci_lo DOUBLE")
    report = validate_against_uc(_spec(f"{registry_db}.results"), spark=spark)
    assert report.has_errors
    joined = "\n".join(report.errors)
    assert "ate" in joined and "double" in joined and "string" in joined
