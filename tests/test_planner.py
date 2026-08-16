"""Tests for the query planner.

The planner takes one or more ModelSpecs plus a list of requested canonical
metric names, and returns a Spark DataFrame with columns
[measurement_key, ..metric_columns.., version?] where metrics have been
resolved through aliases and joined across tables.

Table paths point at Parquet files on disk; the planner reads them via
spark.read.parquet through the data_source seam.
"""

from pathlib import Path

import pandas as pd
import pytest

from causalops import Metric, ModelSpec, Table
from causalops.planner import plan_for_spec, plan_for_specs

# --- fixtures for fake result tables ----------------------------------------


def _write(pdf: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(path, engine="pyarrow", index=False)
    return path


def _seed_result_tables(tmp_path: Path) -> dict[str, Path]:
    """Write the three Parquet tables the planner tests reference."""
    paths = {
        "shared_v3": tmp_path / "shared_v3.parquet",
        "het_v3": tmp_path / "het_v3.parquet",
        "shared_v2": tmp_path / "shared_v2.parquet",
    }
    _write(
        pd.DataFrame(
            {
                "experiment_id": ["e1", "e2"],
                "ate": [0.10, 0.20],
                "ci_lo": [0.05, 0.10],
                "ci_hi": [0.15, 0.30],
            }
        ),
        paths["shared_v3"],
    )
    _write(
        pd.DataFrame({"experiment_id": ["e1", "e2"], "het_score": [0.4, 0.9]}),
        paths["het_v3"],
    )
    _write(
        pd.DataFrame({"experiment_id": ["e1", "e2"], "te": [0.09, 0.19]}),
        paths["shared_v2"],
    )
    return paths


def _spec_v3(paths: dict[str, Path]) -> ModelSpec:
    return ModelSpec(
        family="uplift",
        version="3.0.0",
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path=str(paths["shared_v3"]),
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column="ate", dtype="double", aliases=["te"]),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                    Metric(name="ci_upper", column="ci_hi", dtype="double"),
                ],
            ),
            Table(
                name="het",
                path=str(paths["het_v3"]),
                key="experiment_id",
                metrics=[
                    Metric(name="cate_variance", column="het_score", dtype="double"),
                ],
            ),
        ],
    )


def _spec_v2(paths: dict[str, Path]) -> ModelSpec:
    return ModelSpec(
        family="uplift",
        version="2.9.0",
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path=str(paths["shared_v2"]),
                key="experiment_id",
                metrics=[
                    # v2 used 'te' as the canonical name; in v3 we renamed to
                    # 'treatment_effect'. Alias enables cross-version continuity.
                    Metric(name="treatment_effect", column="te", dtype="double", aliases=["te"]),
                ],
            ),
        ],
    )


# --- single-version tests ---------------------------------------------------


def test_single_version_join_across_tables(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    df = plan_for_spec(spark, _spec_v3(paths), metrics=["treatment_effect", "cate_variance"])
    rows = {r["experiment_id"]: r for r in df.collect()}
    assert set(rows) == {"e1", "e2"}
    assert rows["e1"]["treatment_effect"] == 0.10
    assert rows["e1"]["cate_variance"] == 0.4


def test_single_version_resolves_alias_to_physical_column(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    df = plan_for_spec(spark, _spec_v3(paths), metrics=["te"])  # alias
    assert "treatment_effect" in df.columns
    assert "te" not in df.columns
    assert df.filter("experiment_id = 'e1'").first()["treatment_effect"] == 0.10


def test_unknown_metric_raises(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    with pytest.raises(KeyError, match="unknown metric"):
        plan_for_spec(spark, _spec_v3(paths), metrics=["nope"])


# --- multi-version tests ----------------------------------------------------


def test_multi_version_union_aligns_alias_across_versions(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    df = plan_for_specs(spark, [_spec_v2(paths), _spec_v3(paths)], metrics=["treatment_effect"])
    assert set(df.columns) == {"experiment_id", "treatment_effect", "version"}
    rows = sorted((r["version"], r["experiment_id"], r["treatment_effect"]) for r in df.collect())
    assert rows == [
        ("2.9.0", "e1", 0.09),
        ("2.9.0", "e2", 0.19),
        ("3.0.0", "e1", 0.10),
        ("3.0.0", "e2", 0.20),
    ]


def test_multi_version_pads_missing_metrics_with_null(spark, tmp_path):
    """v2 has no cate_variance; requesting it must union with NULLs, not fail."""
    paths = _seed_result_tables(tmp_path)
    df = plan_for_specs(
        spark,
        [_spec_v2(paths), _spec_v3(paths)],
        metrics=["treatment_effect", "cate_variance"],
    )
    assert set(df.columns) == {
        "experiment_id",
        "treatment_effect",
        "cate_variance",
        "version",
    }
    v2_rows = df.filter("version = '2.9.0'").collect()
    assert all(r["cate_variance"] is None for r in v2_rows)
    v3_rows = df.filter("version = '3.0.0' AND experiment_id = 'e1'").collect()
    assert v3_rows[0]["cate_variance"] == 0.4
