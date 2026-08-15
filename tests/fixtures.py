"""Helpers to build sample ModelSpec objects for tests."""

from causalops import Metric, ModelSpec, Table


def sample_spec(family: str = "uplift", version: str = "3.1.0") -> ModelSpec:
    return ModelSpec(
        family=family,
        version=version,
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path=f"{family}.shared_v{version.split('.')[0]}",
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column="ate", dtype="double", aliases=["te"]),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                    Metric(name="ci_upper", column="ci_hi", dtype="double"),
                ],
            ),
        ],
    )
