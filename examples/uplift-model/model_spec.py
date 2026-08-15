"""Example model_spec.py — imported by the register CLI at tag time."""
from registry_sdk import Metric, ModelSpec, Table

spec = ModelSpec(
    family="uplift",
    version="3.1.0",
    measurement_key="experiment_id",
    tables=[
        Table(
            name="shared",
            path="uplift.shared_v3",
            key="experiment_id",
            metrics=[
                Metric(name="treatment_effect", column="ate",
                       dtype="double", aliases=["te"]),
                Metric(name="ci_lower", column="ci_lo", dtype="double"),
                Metric(name="ci_upper", column="ci_hi", dtype="double"),
            ],
        ),
        Table(
            name="heterogeneity",
            path="uplift.het_v3",
            key="experiment_id",
            metrics=[
                Metric(name="cate_variance", column="het_score",
                       dtype="double", aliases=["heterogeneity_score"]),
            ],
        ),
    ],
)
