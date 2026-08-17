"""Example model spec with mock data generator — imported by the register CLI at tag time.

Table paths point at the local Parquet files produced by `scripts/seed_examples.py`.
When lifting to Databricks, swap these for UC identifiers, e.g.
`"main.uplift.shared_v3"`.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from causalops import Metric, ModelSpec, Table

from ..utils import run_dates as _run_dates
from ..utils import write_parquet

_N_ROWS = 100


def _scm_spec(data_dir: Path, version: str) -> ModelSpec:
    results_path = data_dir / "scm" / "results_v1.parquet"
    metrics_path = data_dir / "scm" / "metrics_v1.parquet"
    return ModelSpec(
        family="scm",
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
                        column="treatment_target_total",
                        dtype="double",
                    ),
                    Metric(
                        name="target_incremental",
                        column="treatment_target_incremental",
                        dtype="double",
                    ),
                    Metric(
                        name="target_incremental_pct",
                        column="treatment_target_incremental_percentage",
                        dtype="double",
                    ),
                    Metric(
                        name="target_ci_hi",
                        column="treatment_target_ci_hi",
                        dtype="double",
                    ),
                    Metric(
                        name="target_ci_lo",
                        column="treatment_target_ci_lo",
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
                        name="treatment_ess_ratio",
                        column="treatment_ess_ratio",
                        dtype="double",
                    ),
                    Metric(
                        name="control_ess_ratio",
                        column="control_ess_ratio",
                        dtype="double",
                    ),
                    Metric(
                        name="absolute_ess",
                        column="absolute_ess",
                        dtype="double",
                    ),
                    Metric(
                        name="smd_feature_pre_period_upc_purchases_L3M",
                        column="smd_feature_pre_period_upc_purchases_L3M",
                        dtype="double",
                    ),
                ),
            ),
        ),
    )


def seed_scm_model(data_dir: Path) -> None:
    """Mock tables for a synthetic-control model (SCM) example.

    Referenced tables:

      scm/results_v1.parquet
        rid                                      : string   (key)
        run_date                                 : string
        run_id                                   : string
        treatment_target_total                   : double
        treatment_target_incremental             : double
        treatment_target_incremental_percentage  : double
        treatment_target_coeff                   : double   (scm specific)
        treatment_target_ci_hi                   : double
        treatment_target_ci_lo                   : double

      scm/metrics_v1.parquet
        rid                                        : string   (key)
        run_date                                   : string
        run_id                                     : string
        treatment_ess_ratio                        : double
        control_ess_ratio                          : double
        absolute_ess                               : double
        smd_feature_pre_period_upc_purchases_L3M   : double
    """
    rng = np.random.default_rng(seed=17)
    rids = [f"scm_{i:03d}" for i in range(1, _N_ROWS + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=_N_ROWS)]
    run_dates = _run_dates(_N_ROWS)

    totals = rng.normal(loc=100_000, scale=15_000, size=_N_ROWS)
    incremental = rng.normal(loc=8_000, scale=2_000, size=_N_ROWS)
    ci_half = rng.uniform(500, 2_000, size=_N_ROWS)
    write_parquet(
        pd.DataFrame(
            {
                "rid": rids,
                "run_date": run_dates,
                "run_id": run_ids,
                "treatment_target_total": totals,
                "treatment_target_incremental": incremental,
                "treatment_target_incremental_percentage": incremental / totals,
                "treatment_target_coeff": rng.uniform(0.6, 1.2, size=_N_ROWS),
                "treatment_target_ci_hi": incremental + ci_half,
                "treatment_target_ci_lo": incremental - ci_half,
            }
        ),
        data_dir / "scm" / "results_v1.parquet",
    )

    write_parquet(
        pd.DataFrame(
            {
                "rid": rids,
                "run_date": run_dates,
                "run_id": run_ids,
                "treatment_ess_ratio": rng.uniform(0.5, 1.0, size=_N_ROWS),
                "control_ess_ratio": rng.uniform(0.5, 1.0, size=_N_ROWS),
                "absolute_ess": rng.uniform(50, 500, size=_N_ROWS),
                "smd_feature_pre_period_upc_purchases_L3M": rng.normal(0, 0.1, size=_N_ROWS),
            }
        ),
        data_dir / "scm" / "metrics_v1.parquet",
    )
