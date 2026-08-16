"""Example model_spec.py — imported by the register CLI at tag time.

Table paths point at the local Parquet files produced by `scripts/seed_examples.py`.
When lifting to Databricks, swap these for UC identifiers, e.g.
`"main.uplift.shared_v3"`.
"""

from causalops import Metric, ModelSpec, Table
from causalops.paths import default_data_dir

_UPLIFT = default_data_dir() / "uplift"

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
                Metric(name="treatment_effect", column="ate", dtype="double", aliases=["te"]),
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
