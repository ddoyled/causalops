"""Example model spec with mock data generator — imported by the register CLI at tag time.

Table paths point at the local Parquet files produced by `scripts/seed_examples.py`.
When lifting to Databricks, swap these for UC identifiers, e.g.
`"main.uplift.shared_v3"`.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from causalops import Metric, ModelSpec, Table
from causalops.paths import default_data_dir

from ..utils import run_dates as _run_dates
from ..utils import write_parquet

_UPLIFT = default_data_dir() / "uplift"
_N_ROWS = 100


def _bsts_spec(data_dir: Path, version: str) -> ModelSpec:
    results_path = data_dir / "bsts" / "results_v1.parquet"
    metrics_path = data_dir / "bsts" / "metrics_v1.parquet"
    return ModelSpec(
        family="bsts",
        version=version,
        measurement_key="rid",
        tables=(
            Table(
                name="results",
                path=str(results_path),
                key="rid",
                metrics=(
                    Metric(
                        name="target_total",
                        column="observed_target_total",
                        dtype="double",
                    ),
                    Metric(
                        name="target_incremental",
                        column="observed_target_incremental",
                        dtype="double",
                    ),
                    Metric(
                        name="target_incremental_pct",
                        column="observed_target_incremental_percentage",
                        dtype="double",
                    ),
                    Metric(
                        name="target_ci_hi",
                        column="observed_target_ci_hi",
                        dtype="double",
                    ),
                    Metric(
                        name="target_ci_lo",
                        column="observed_target_ci_lo",
                        dtype="double",
                    ),
                ),
            ),
            Table(
                name="metrics",
                path=str(metrics_path),
                key="rid",
                metrics=(
                    Metric(
                        name="seasonality_impact_p50",
                        column="seasonality_impact_p50",
                        dtype="double",
                    ),
                    Metric(
                        name="seasonality_impact_p025",
                        column="seasonality_impact_p025",
                        dtype="double",
                    ),
                    Metric(
                        name="seasonality_impact_p975",
                        column="seasonality_impact_p975",
                        dtype="double",
                    ),
                    Metric(
                        name="trend_impact_p50",
                        column="trend_impact_p50",
                        dtype="double",
                    ),
                    Metric(
                        name="trend_impact_p025",
                        column="trend_impact_p025",
                        dtype="double",
                    ),
                    Metric(
                        name="trend_impact_p975",
                        column="trend_impact_p975",
                        dtype="double",
                    ),
                    Metric(
                        name="category_purchases_impact_p50",
                        column="category_purchases_impact_p50",
                        dtype="double",
                    ),
                    Metric(
                        name="category_purchases_impact_p025",
                        column="category_purchases_impact_p025",
                        dtype="double",
                    ),
                    Metric(
                        name="category_purchases_impact_p975",
                        column="category_purchases_impact_p975",
                        dtype="double",
                    ),
                ),
            ),
        ),
    )


def seed_bsts_model(data_dir: Path) -> None:
    """Mock tables for a Bayesian structural time-series (BSTS) example.

    Referenced tables:

      bsts/results_v1.parquet
        rid                                      : string   (key)
        run_date                                 : string
        run_id                                   : string
        observed_target_total                    : double
        observed_target_incremental              : double
        observed_target_incremental_percentage   : double
        observed_target_ci_hi                    : double
        observed_target_ci_lo                    : double

      bsts/metrics_v1.parquet
        rid                              : string   (key)
        run_date                         : string
        run_id                           : string
        seasonality_impact_p50           : double
        ...
    """
    rng = np.random.default_rng(seed=91)
    rids = [f"bsts_{i:03d}" for i in range(1, _N_ROWS + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=_N_ROWS)]
    run_dates = _run_dates(_N_ROWS)

    totals = rng.normal(loc=100_000, scale=15_000, size=_N_ROWS)
    incremental = rng.normal(loc=6_500, scale=1_500, size=_N_ROWS)
    ci_half = rng.uniform(400, 1_500, size=_N_ROWS)
    write_parquet(
        pd.DataFrame(
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
        ),
        data_dir / "bsts" / "results_v1.parquet",
    )

    def _band(loc: float, scale: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        p50 = rng.normal(loc=loc, scale=scale, size=_N_ROWS)
        half = rng.uniform(scale * 0.5, scale * 1.5, size=_N_ROWS)
        return p50 - half, p50, p50 + half

    seasonality_lo, seasonality_p50, seasonality_hi = _band(0.02, 0.005)
    trend_lo, trend_p50, trend_hi = _band(0.01, 0.003)
    category_lo, category_p50, category_hi = _band(0.03, 0.008)

    write_parquet(
        pd.DataFrame(
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
        ),
        data_dir / "bsts" / "metrics_v1.parquet",
    )
