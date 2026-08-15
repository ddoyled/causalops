"""Seed the local Delta warehouse with mock tables for the examples.

Registry-of-generators pattern: each `examples/<name>/` gets an entry in
`GENERATORS`; the entry is a callable that receives a Spark session and writes
its example's tables. Add new examples by writing a `seed_<name>` function
and registering it below.

Usage
-----
    # Seed one example:
    python scripts/seed_examples.py --example uplift-model

    # Seed everything registered:
    python scripts/seed_examples.py --example all

By default this writes into the same warehouse the CLI uses
(`<repo-root>/.causalops/warehouse`), so subsequent `causalops register` runs
see the tables. Override with `--warehouse-dir` / `--metastore-dir` for a
scratch workspace.

Paths are 2-part (`<db>.<table>`) — matches the current POC's local-Spark
target. If/when we move to Unity Catalog, spec paths become 3-part
(`catalog.db.table`) and the same `_write_delta` call works unchanged (the
first segment just becomes a UC catalog instead of a Hive database).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import click
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from causalops.paths import default_metastore_dir, default_warehouse_dir
from causalops.spark_session import build_local_spark_session

# Rows per mock table. Small enough to keep seed runs fast, large enough to
# exercise Spark's group-by / aggregation paths.
N_ROWS = 100

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_delta(spark: SparkSession, df, path: str) -> int:
    """Write `df` as a Delta table at `path` (2-part `<db>.<table>`).

    Creates the database if needed. Idempotent: `mode=overwrite` replaces any
    prior seeded content. Returns the row count for the caller's log line.
    """
    db, _ = path.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    df.write.format("delta").mode("overwrite").saveAsTable(path)
    count = spark.table(path).count()
    click.echo(f"  wrote {path} ({count} rows)")
    return count


def _run_dates(n: int, start: str = "2026-01-01") -> list[str]:
    return pd.date_range(start, periods=n, freq="D").strftime("%Y-%m-%d").tolist()


def _spark_from_pandas(spark: SparkSession, pdf: pd.DataFrame, schema: list[str]):
    """Build a Spark DataFrame from a pandas one via records.

    Avoids `spark.createDataFrame(pdf, ...)` — its pandas path in PySpark 3.5.2
    imports `distutils`, which was removed in Python 3.12.
    """
    # np scalars → Python scalars so Spark's Java gateway accepts them.
    records = [tuple(row) for row in pdf.itertuples(index=False, name=None)]
    return spark.createDataFrame(records, schema=", ".join(schema))


# ---------------------------------------------------------------------------
# Generators — one per example. Fill in the DataFrames.
# ---------------------------------------------------------------------------


def seed_uplift_model(spark: SparkSession) -> None:
    """Mock tables for examples/uplift-model/model_spec.py.

    Referenced tables (see the spec for the source of truth):

      uplift.shared_v3
        experiment_id : string   (key)
        ate           : double   (metric: treatment_effect / te)
        ci_lo         : double   (metric: ci_lower)
        ci_hi         : double   (metric: ci_upper)

      uplift.het_v3
        experiment_id : string   (key)
        het_score     : double   (metric: cate_variance / heterogeneity_score)
    """
    rng = np.random.default_rng(seed=42)
    experiment_ids = [f"exp_{i:03d}" for i in range(1, N_ROWS + 1)]

    shared_schema = [
        "experiment_id STRING",
        "ate DOUBLE",
        "ci_lo DOUBLE",
        "ci_hi DOUBLE",
    ]
    ate = rng.normal(loc=0.04, scale=0.015, size=N_ROWS)
    half = rng.uniform(0.005, 0.02, size=N_ROWS)
    shared_pdf = pd.DataFrame(
        {
            "experiment_id": experiment_ids,
            "ate": ate,
            "ci_lo": ate - half,
            "ci_hi": ate + half,
        }
    )
    shared_df = _spark_from_pandas(spark, shared_pdf, shared_schema)
    _write_delta(spark, shared_df, "uplift.shared_v3")

    het_schema = [
        "experiment_id STRING",
        "het_score DOUBLE",
    ]
    het_pdf = pd.DataFrame(
        {
            "experiment_id": experiment_ids,
            "het_score": rng.uniform(0.05, 0.25, size=N_ROWS),
        }
    )
    het_df = _spark_from_pandas(spark, het_pdf, het_schema)
    _write_delta(spark, het_df, "uplift.het_v3")


def seed_scm_model(spark: SparkSession) -> None:
    """Mock tables for a synthetic-control model (SCM) example.

    Referenced tables:

      scm.results_v1
        rid                                      : string   (key)
        run_date                                 : string
        run_id                                   : string
        treatment_target_total                   : double
        treatment_target_incremental             : double
        treatment_target_incremental_percentage  : double
        treatment_target_coeff                   : double   (scm specific)
        treatment_target_ci_hi                   : double
        treatment_target_ci_lo                   : double

      scm.metrics_v1
        rid                                        : string   (key)
        run_date                                   : string
        run_id                                     : string
        treatment_ess_ratio                        : double
        control_ess_ratio                          : double
        absolute_ess                               : double
        smd_feature_pre_period_upc_purchases_L3M   : double
    """
    rng = np.random.default_rng(seed=17)
    rids = [f"scm_{i:03d}" for i in range(1, N_ROWS + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=N_ROWS)]
    run_dates = _run_dates(N_ROWS)

    results_table_name = "scm.results_v1"
    results_schema = [
        "rid STRING",
        "run_date STRING",
        "run_id STRING",
        "treatment_target_total DOUBLE",
        "treatment_target_incremental DOUBLE",
        "treatment_target_incremental_percentage DOUBLE",
        "treatment_target_coeff DOUBLE",
        "treatment_target_ci_hi DOUBLE",
        "treatment_target_ci_lo DOUBLE",
    ]
    totals = rng.normal(loc=100_000, scale=15_000, size=N_ROWS)
    incremental = rng.normal(loc=8_000, scale=2_000, size=N_ROWS)
    ci_half = rng.uniform(500, 2_000, size=N_ROWS)
    results_pdf = pd.DataFrame(
        {
            "rid": rids,
            "run_date": run_dates,
            "run_id": run_ids,
            "treatment_target_total": totals,
            "treatment_target_incremental": incremental,
            "treatment_target_incremental_percentage": incremental / totals,
            "treatment_target_coeff": rng.uniform(0.6, 1.2, size=N_ROWS),
            "treatment_target_ci_hi": incremental + ci_half,
            "treatment_target_ci_lo": incremental - ci_half,
        }
    )
    results_df = _spark_from_pandas(spark, results_pdf, results_schema)
    _write_delta(spark, results_df, results_table_name)

    metrics_table_name = "scm.metrics_v1"
    metrics_schema = [
        "rid STRING",
        "run_date STRING",
        "run_id STRING",
        "treatment_ess_ratio DOUBLE",
        "control_ess_ratio DOUBLE",
        "absolute_ess DOUBLE",
        "smd_feature_pre_period_upc_purchases_L3M DOUBLE",
    ]
    metrics_pdf = pd.DataFrame(
        {
            "rid": rids,
            "run_date": run_dates,
            "run_id": run_ids,
            "treatment_ess_ratio": rng.uniform(0.5, 1.0, size=N_ROWS),
            "control_ess_ratio": rng.uniform(0.5, 1.0, size=N_ROWS),
            "absolute_ess": rng.uniform(50, 500, size=N_ROWS),
            "smd_feature_pre_period_upc_purchases_L3M": rng.normal(0, 0.1, size=N_ROWS),
        }
    )
    metrics_df = _spark_from_pandas(spark, metrics_pdf, metrics_schema)
    _write_delta(spark, metrics_df, metrics_table_name)


def seed_bsts_model(spark: SparkSession) -> None:
    """Mock tables for a Bayesian structural time-series (BSTS) example.

    Referenced tables:

      bsts.results_v1
        rid                                      : string   (key)
        run_date                                 : string
        run_id                                   : string
        observed_target_total                    : double
        observed_target_incremental              : double
        observed_target_incremental_percentage   : double
        observed_target_ci_hi                    : double
        observed_target_ci_lo                    : double

      bsts.metrics_v1
        rid                              : string   (key)
        run_date                         : string
        run_id                           : string
        seasonality_impact_p50           : double
        seasonality_impact_p025          : double
        seasonality_impact_p975          : double
        trend_impact_p50                 : double
        trend_impact_p025                : double
        trend_impact_p975                : double
        category_purchases_impact_p50    : double
        category_purchases_impact_p025   : double
        category_purchases_impact_p975   : double
    """
    rng = np.random.default_rng(seed=91)
    rids = [f"bsts_{i:03d}" for i in range(1, N_ROWS + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=N_ROWS)]
    run_dates = _run_dates(N_ROWS)

    results_table_name = "bsts.results_v1"
    results_schema = [
        "rid STRING",
        "run_date STRING",
        "run_id STRING",
        "observed_target_total DOUBLE",
        "observed_target_incremental DOUBLE",
        "observed_target_incremental_percentage DOUBLE",
        "observed_target_ci_hi DOUBLE",
        "observed_target_ci_lo DOUBLE",
    ]
    totals = rng.normal(loc=100_000, scale=15_000, size=N_ROWS)
    incremental = rng.normal(loc=6_500, scale=1_500, size=N_ROWS)
    ci_half = rng.uniform(400, 1_500, size=N_ROWS)
    results_pdf = pd.DataFrame(
        {
            "rid": rids,
            "run_date": run_dates,
            "run_id": run_ids,
            "observed_target_total": totals,
            "observed_target_incremental": incremental,
            "observed_target_incremental_percentage": incremental / totals,
            "observed_target_ci_hi": incremental + ci_half,
            "observed_target_ci_lo": incremental - ci_half,
        }
    )
    results_df = _spark_from_pandas(spark, results_pdf, results_schema)
    _write_delta(spark, results_df, results_table_name)

    def _band(loc: float, scale: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        p50 = rng.normal(loc=loc, scale=scale, size=N_ROWS)
        half = rng.uniform(scale * 0.5, scale * 1.5, size=N_ROWS)
        return p50 - half, p50, p50 + half

    seasonality_lo, seasonality_p50, seasonality_hi = _band(0.02, 0.005)
    trend_lo, trend_p50, trend_hi = _band(0.01, 0.003)
    category_lo, category_p50, category_hi = _band(0.03, 0.008)

    metrics_table_name = "bsts.metrics_v1"
    metrics_schema = [
        "rid STRING",
        "run_date STRING",
        "run_id STRING",
        "seasonality_impact_p50 DOUBLE",
        "seasonality_impact_p025 DOUBLE",
        "seasonality_impact_p975 DOUBLE",
        "trend_impact_p50 DOUBLE",
        "trend_impact_p025 DOUBLE",
        "trend_impact_p975 DOUBLE",
        "category_purchases_impact_p50 DOUBLE",
        "category_purchases_impact_p025 DOUBLE",
        "category_purchases_impact_p975 DOUBLE",
    ]
    metrics_pdf = pd.DataFrame(
        {
            "rid": rids,
            "run_date": run_dates,
            "run_id": run_ids,
            "seasonality_impact_p50": seasonality_p50,
            "seasonality_impact_p025": seasonality_lo,
            "seasonality_impact_p975": seasonality_hi,
            "trend_impact_p50": trend_p50,
            "trend_impact_p025": trend_lo,
            "trend_impact_p975": trend_hi,
            "category_purchases_impact_p50": category_p50,
            "category_purchases_impact_p025": category_lo,
            "category_purchases_impact_p975": category_hi,
        }
    )
    metrics_df = _spark_from_pandas(spark, metrics_pdf, metrics_schema)
    _write_delta(spark, metrics_df, metrics_table_name)


# Add new examples here. Key MUST match the directory name under examples/.
GENERATORS: dict[str, Callable[[SparkSession], None]] = {
    "uplift-model": seed_uplift_model,
    "scm-model": seed_scm_model,
    "bsts-model": seed_bsts_model,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command()
@click.option(
    "--example",
    default="all",
    show_default=True,
    help="Example name (matches examples/<name>/) or 'all'.",
)
@click.option(
    "--warehouse-dir",
    type=click.Path(path_type=Path),
    default=default_warehouse_dir(),
    envvar="CAUSALOPS_WAREHOUSE_DIR",
    show_default=True,
)
@click.option(
    "--metastore-dir",
    type=click.Path(path_type=Path),
    default=default_metastore_dir(),
    envvar="CAUSALOPS_METASTORE_DIR",
    show_default=True,
)
def main(example: str, warehouse_dir: Path, metastore_dir: Path) -> None:
    """Seed the local warehouse with example mock tables."""
    if example != "all" and example not in GENERATORS:
        known = ", ".join(sorted(GENERATORS)) or "(none registered)"
        raise click.ClickException(f"unknown example {example!r}. Known: {known}")

    to_run = list(GENERATORS.items()) if example == "all" else [(example, GENERATORS[example])]

    spark = build_local_spark_session(
        warehouse_dir=warehouse_dir,
        metastore_dir=metastore_dir,
        app_name="causalops_seed_examples",
    )
    try:
        for name, generator in to_run:
            click.echo(f"seeding {name} ...")
            generator(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
