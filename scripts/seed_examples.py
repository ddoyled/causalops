"""Seed the local data directory with mock Parquet tables for the examples.

Registry-of-generators pattern: each `examples/<name>/` gets an entry in
`GENERATORS`; the entry is a callable that receives the example's data
directory and writes its tables as Parquet files.

Usage
-----
    # Seed one example:
    python scripts/seed_examples.py --example uplift-model

    # Seed everything registered:
    python scripts/seed_examples.py --example all

Writes into `<repo-root>/.causalops/data/<example>/<table>.parquet` by
default. The example's `model_spec.py` points at the same paths, so
`causalops register` reads them straight off the filesystem via
`spark.read.parquet` — no Hive metastore involved.

Paths are filesystem paths in local dev. When lifting to Databricks, swap
them for UC identifiers (`catalog.schema.table`) — the planner treats a
`db.table` string as a `spark.table(...)` call and falls back to
`spark.read.parquet(...)` for anything that looks like a filesystem path.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import click
import numpy as np
import pandas as pd

from causalops.paths import default_data_dir

# Rows per mock table. Small enough to keep seed runs fast, large enough to
# exercise Spark's group-by / aggregation paths downstream.
N_ROWS = 100

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(pdf: pd.DataFrame, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(path, engine="pyarrow", index=False)
    click.echo(f"  wrote {path} ({len(pdf)} rows)")
    return len(pdf)


def _run_dates(n: int, start: str = "2026-01-01") -> list[str]:
    return pd.date_range(start, periods=n, freq="D").strftime("%Y-%m-%d").tolist()


# ---------------------------------------------------------------------------
# Generators — one per example.
# ---------------------------------------------------------------------------


def seed_uplift_model(data_dir: Path) -> None:
    """Mock tables for examples/uplift-model/model_spec.py.

    Referenced tables (see the spec for the source of truth):

      uplift/shared_v3.parquet
        experiment_id : string   (key)
        ate           : double   (metric: treatment_effect / te)
        ci_lo         : double   (metric: ci_lower)
        ci_hi         : double   (metric: ci_upper)

      uplift/het_v3.parquet
        experiment_id : string   (key)
        het_score     : double   (metric: cate_variance / heterogeneity_score)
    """
    rng = np.random.default_rng(seed=42)
    experiment_ids = [f"exp_{i:03d}" for i in range(1, N_ROWS + 1)]

    ate = rng.normal(loc=0.04, scale=0.015, size=N_ROWS)
    half = rng.uniform(0.005, 0.02, size=N_ROWS)
    _write_parquet(
        pd.DataFrame(
            {
                "experiment_id": experiment_ids,
                "ate": ate,
                "ci_lo": ate - half,
                "ci_hi": ate + half,
            }
        ),
        data_dir / "uplift" / "shared_v3.parquet",
    )

    _write_parquet(
        pd.DataFrame(
            {
                "experiment_id": experiment_ids,
                "het_score": rng.uniform(0.05, 0.25, size=N_ROWS),
            }
        ),
        data_dir / "uplift" / "het_v3.parquet",
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
    rids = [f"scm_{i:03d}" for i in range(1, N_ROWS + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=N_ROWS)]
    run_dates = _run_dates(N_ROWS)

    totals = rng.normal(loc=100_000, scale=15_000, size=N_ROWS)
    incremental = rng.normal(loc=8_000, scale=2_000, size=N_ROWS)
    ci_half = rng.uniform(500, 2_000, size=N_ROWS)
    _write_parquet(
        pd.DataFrame(
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
        ),
        data_dir / "scm" / "results_v1.parquet",
    )

    _write_parquet(
        pd.DataFrame(
            {
                "rid": rids,
                "run_date": run_dates,
                "run_id": run_ids,
                "treatment_ess_ratio": rng.uniform(0.5, 1.0, size=N_ROWS),
                "control_ess_ratio": rng.uniform(0.5, 1.0, size=N_ROWS),
                "absolute_ess": rng.uniform(50, 500, size=N_ROWS),
                "smd_feature_pre_period_upc_purchases_L3M": rng.normal(0, 0.1, size=N_ROWS),
            }
        ),
        data_dir / "scm" / "metrics_v1.parquet",
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
    rids = [f"bsts_{i:03d}" for i in range(1, N_ROWS + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=N_ROWS)]
    run_dates = _run_dates(N_ROWS)

    totals = rng.normal(loc=100_000, scale=15_000, size=N_ROWS)
    incremental = rng.normal(loc=6_500, scale=1_500, size=N_ROWS)
    ci_half = rng.uniform(400, 1_500, size=N_ROWS)
    _write_parquet(
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
        p50 = rng.normal(loc=loc, scale=scale, size=N_ROWS)
        half = rng.uniform(scale * 0.5, scale * 1.5, size=N_ROWS)
        return p50 - half, p50, p50 + half

    seasonality_lo, seasonality_p50, seasonality_hi = _band(0.02, 0.005)
    trend_lo, trend_p50, trend_hi = _band(0.01, 0.003)
    category_lo, category_p50, category_hi = _band(0.03, 0.008)

    _write_parquet(
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


# Add new examples here. Key MUST match the directory name under examples/.
GENERATORS: dict[str, Callable[[Path], None]] = {
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
    "--data-dir",
    type=click.Path(path_type=Path),
    default=default_data_dir(),
    envvar="CAUSALOPS_DATA_DIR",
    show_default=True,
)
def main(example: str, data_dir: Path) -> None:
    """Seed the local data dir with example mock Parquet tables."""
    if example != "all" and example not in GENERATORS:
        known = ", ".join(sorted(GENERATORS)) or "(none registered)"
        raise click.ClickException(f"unknown example {example!r}. Known: {known}")

    to_run = list(GENERATORS.items()) if example == "all" else [(example, GENERATORS[example])]
    for name, generator in to_run:
        click.echo(f"seeding {name} ...")
        generator(data_dir)


if __name__ == "__main__":
    main()
