"""Example model spec with mock data generator — imported by the register CLI at tag time.

Table paths point at the local Parquet files produced by `scripts/seed_examples.py`.
When lifting to Databricks, swap these for UC identifiers, e.g.
`"main.uplift.shared_v3"`.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from causalops import Metric, ModelSpec, Table

from ..utils import write_parquet


def uplift_spec(data_dir: Path, version: str) -> ModelSpec:
    shared_path = data_dir / "uplift" / f"shared_v{version}.parquet"
    het_path = data_dir / "uplift" / f"het_v{version}.parquet"
    return ModelSpec(
        family="uplift",
        version=version,
        measurement_key="experiment_id",
        tables=(
            Table(
                name="shared",
                path=str(shared_path),
                key="experiment_id",
                metrics=(
                    Metric(
                        name="treatment_effect",
                        column="ate",
                        dtype="double",
                        aliases=["te"],
                    ),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                    Metric(name="ci_upper", column="ci_hi", dtype="double"),
                ),
            ),
            Table(
                name="heterogeneity",
                path=str(het_path),
                key="experiment_id",
                metrics=(
                    Metric(
                        name="cate_variance",
                        column="het_score",
                        dtype="double",
                        aliases=["heterogeneity_score"],
                    ),
                ),
            ),
        ),
    )


def seed_uplift_model(
    data_dir: Path,
    version: str,
    run_dates: list[str],
    *,
    channel_id: str | None = None,
    seed: int = 42,
) -> None:
    """Mock tables for an uplift-model example.

    Referenced tables:

      uplift/shared_v1.parquet
        experiment_id : string   (key)
        run_date      : string
        run_id        : string
        ate           : double   (metric: treatment_effect / te)
        ci_lo         : double   (metric: ci_lower)
        ci_hi         : double   (metric: ci_upper)

      uplift/het_v1.parquet
        experiment_id : string   (key)
        run_date      : string
        run_id        : string
        het_score     : double   (metric: cate_variance / heterogeneity_score)
    """
    n = len(run_dates)
    rng = np.random.default_rng(seed=seed)
    experiment_ids = [f"exp_{i:03d}" for i in range(1, n + 1)]
    run_ids = [f"run_{i:04d}" for i in rng.integers(1000, 9999, size=n)]
    channel_col = {"channel_id": [channel_id] * n} if channel_id is not None else {}

    ate = rng.normal(loc=0.04, scale=0.015, size=n)
    half = rng.uniform(0.005, 0.02, size=n)
    write_parquet(
        pd.DataFrame(
            {
                "experiment_id": experiment_ids,
                "run_date": run_dates,
                "run_id": run_ids,
                **channel_col,
                "ate": ate,
                "ci_lo": ate - half,
                "ci_hi": ate + half,
            }
        ),
        data_dir / "uplift" / f"shared_v{version}.parquet",
    )

    write_parquet(
        pd.DataFrame(
            {
                "experiment_id": experiment_ids,
                "run_date": run_dates,
                "run_id": run_ids,
                **channel_col,
                "het_score": rng.uniform(0.05, 0.25, size=n),
            }
        ),
        data_dir / "uplift" / f"het_v{version}.parquet",
    )
