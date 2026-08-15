"""Tests for the query planner.

The planner takes one or more ModelSpecs plus a list of requested canonical
metric names, and returns a Spark DataFrame with columns
[measurement_key, ..metric_columns.., version?] where metrics have been
resolved through aliases and joined across tables.
"""
import pytest

from causalops import Metric, ModelSpec, Table
from causalops.planner import plan_for_spec, plan_for_specs


# --- fixtures for fake result tables ----------------------------------------

def _seed_result_tables(spark, db):
    """Create fake result tables in the local warehouse for planner tests."""
    # v3 shared: canonical column 'ate'
    spark.createDataFrame(
        [("e1", 0.10, 0.05, 0.15), ("e2", 0.20, 0.10, 0.30)],
        schema="experiment_id STRING, ate DOUBLE, ci_lo DOUBLE, ci_hi DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v3")
    # v3 heterogeneity
    spark.createDataFrame(
        [("e1", 0.4), ("e2", 0.9)],
        schema="experiment_id STRING, het_score DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.het_v3")
    # v2 shared: physical column 'te' (former canonical name)
    spark.createDataFrame(
        [("e1", 0.09), ("e2", 0.19)],
        schema="experiment_id STRING, te DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v2")


def _spec_v3(db):
    return ModelSpec(
        family="uplift", version="3.0.0", measurement_key="experiment_id",
        tables=[
            Table(name="shared", path=f"{db}.shared_v3", key="experiment_id", metrics=[
                Metric(name="treatment_effect", column="ate",
                       dtype="double", aliases=["te"]),
                Metric(name="ci_lower", column="ci_lo", dtype="double"),
                Metric(name="ci_upper", column="ci_hi", dtype="double"),
            ]),
            Table(name="het", path=f"{db}.het_v3", key="experiment_id", metrics=[
                Metric(name="cate_variance", column="het_score", dtype="double"),
            ]),
        ],
    )


def _spec_v2(db):
    return ModelSpec(
        family="uplift", version="2.9.0", measurement_key="experiment_id",
        tables=[
            Table(name="shared", path=f"{db}.shared_v2", key="experiment_id", metrics=[
                # v2 used 'te' as the canonical name; in v3 we renamed to
                # 'treatment_effect'. Alias enables cross-version continuity.
                Metric(name="treatment_effect", column="te",
                       dtype="double", aliases=["te"]),
            ]),
        ],
    )


# --- single-version tests ---------------------------------------------------

def test_single_version_join_across_tables(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    spec = _spec_v3(registry_db)
    df = plan_for_spec(spark, spec, metrics=["treatment_effect", "cate_variance"])
    rows = {r["experiment_id"]: r for r in df.collect()}
    assert set(rows) == {"e1", "e2"}
    assert rows["e1"]["treatment_effect"] == 0.10
    assert rows["e1"]["cate_variance"] == 0.4


def test_single_version_resolves_alias_to_physical_column(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    spec = _spec_v3(registry_db)
    df = plan_for_spec(spark, spec, metrics=["te"])  # alias
    assert "treatment_effect" in df.columns
    assert "te" not in df.columns
    assert df.filter("experiment_id = 'e1'").first()["treatment_effect"] == 0.10


def test_unknown_metric_raises(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    spec = _spec_v3(registry_db)
    with pytest.raises(KeyError, match="unknown metric"):
        plan_for_spec(spark, spec, metrics=["nope"])


# --- multi-version tests ----------------------------------------------------

def test_multi_version_union_aligns_alias_across_versions(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    v2, v3 = _spec_v2(registry_db), _spec_v3(registry_db)
    df = plan_for_specs(spark, [v2, v3], metrics=["treatment_effect"])
    assert set(df.columns) == {"experiment_id", "treatment_effect", "version"}
    rows = sorted(
        (r["version"], r["experiment_id"], r["treatment_effect"])
        for r in df.collect()
    )
    assert rows == [
        ("2.9.0", "e1", 0.09), ("2.9.0", "e2", 0.19),
        ("3.0.0", "e1", 0.10), ("3.0.0", "e2", 0.20),
    ]


def test_multi_version_pads_missing_metrics_with_null(spark, registry_db):
    """v2 has no cate_variance; requesting it must union with NULLs, not fail."""
    _seed_result_tables(spark, registry_db)
    v2, v3 = _spec_v2(registry_db), _spec_v3(registry_db)
    df = plan_for_specs(spark, [v2, v3], metrics=["treatment_effect", "cate_variance"])
    assert set(df.columns) == {
        "experiment_id", "treatment_effect", "cate_variance", "version",
    }
    v2_rows = df.filter("version = '2.9.0'").collect()
    assert all(r["cate_variance"] is None for r in v2_rows)
    v3_rows = df.filter("version = '3.0.0' AND experiment_id = 'e1'").collect()
    assert v3_rows[0]["cate_variance"] == 0.4
