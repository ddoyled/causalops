"""Example model spec with mock data generator — imported by the register CLI at tag time.

Table paths point at the local Parquet files produced by `scripts/seed_examples.py`.
When lifting to Databricks, swap these for UC identifiers, e.g.
`"main.uplift.shared_v3"`.
"""

from pathlib import Path

import numpy as np
import pandas as pd
from examples.utils import write_parquet

from causalops import Metric, ModelSpec, Table
from causalops.paths import default_data_dir

_UPLIFT = default_data_dir() / "uplift"
_N_ROWS = 100

spec = ModelSpec(
    family="uplift",
    version="3.1.0",
    measurement_key="experiment_id",
    tables=[
        Table(
            name="shared",
            path=str(_UPLIFT / "shared_v3.parquet"),
            key="experiment_id",
            metrics=[
                Metric(
                    name="treatment_effect",
                    column="ate",
                    dtype="double",
                    aliases=["te"],
                ),
                Metric(name="ci_lower", column="ci_lo", dtype="double"),
                Metric(name="ci_upper", column="ci_hi", dtype="double"),
            ],
        ),
        Table(
            name="heterogeneity",
            path=str(_UPLIFT / "het_v3.parquet"),
            key="experiment_id",
            metrics=[
                Metric(
                    name="cate_variance",
                    column="het_score",
                    dtype="double",
                    aliases=["heterogeneity_score"],
                ),
            ],
        ),
    ],
)


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
    experiment_ids = [f"exp_{i:03d}" for i in range(1, _N_ROWS + 1)]

    ate = rng.normal(loc=0.04, scale=0.015, size=_N_ROWS)
    half = rng.uniform(0.005, 0.02, size=_N_ROWS)
    write_parquet(
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

    write_parquet(
        pd.DataFrame(
            {
                "experiment_id": experiment_ids,
                "het_score": rng.uniform(0.05, 0.25, size=_N_ROWS),
            }
        ),
        data_dir / "uplift" / "het_v3.parquet",
    )
