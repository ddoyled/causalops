"""Tests for ModelSpec / Table / Metric Pydantic models."""
import json

import pytest
from pydantic import ValidationError

from registry_sdk import Metric, ModelSpec, Table


def _sample_spec() -> ModelSpec:
    return ModelSpec(
        family="uplift",
        version="3.1.0",
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path="analytics.uplift.shared_v3",
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column="ate",
                           dtype="double", aliases=["te"]),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                ],
            ),
            Table(
                name="heterogeneity",
                path="analytics.uplift.het_v3",
                key="experiment_id",
                metrics=[
                    Metric(name="cate_variance", column="het_score",
                           dtype="double", aliases=["heterogeneity_score"]),
                ],
            ),
        ],
    )


def test_spec_constructs_and_exposes_fields():
    spec = _sample_spec()
    assert spec.family == "uplift"
    assert spec.version == "3.1.0"
    assert len(spec.tables) == 2
    assert spec.tables[0].metrics[0].aliases == ("te",)


def test_spec_round_trips_through_json():
    spec = _sample_spec()
    payload = spec.model_dump_json()
    restored = ModelSpec.model_validate_json(payload)
    assert restored == spec
    # sanity: JSON is valid + stable-ish
    assert json.loads(payload)["family"] == "uplift"


def test_version_must_be_semver_like():
    with pytest.raises(ValidationError):
        ModelSpec(
            family="uplift", version="v3.1", measurement_key="x",
            tables=[Table(name="t", path="a.b.c", key="x",
                          metrics=[Metric(name="m", column="c", dtype="double")])],
        )


def test_duplicate_metric_names_within_table_rejected():
    with pytest.raises(ValidationError, match="duplicate metric"):
        Table(
            name="t", path="a.b.c", key="x",
            metrics=[
                Metric(name="m", column="c1", dtype="double"),
                Metric(name="m", column="c2", dtype="double"),
            ],
        )


def test_alias_colliding_with_canonical_in_same_table_rejected():
    with pytest.raises(ValidationError, match="alias .* collides"):
        Table(
            name="t", path="a.b.c", key="x",
            metrics=[
                Metric(name="ate", column="ate", dtype="double"),
                Metric(name="treatment_effect", column="te_col",
                       dtype="double", aliases=["ate"]),
            ],
        )


def test_duplicate_table_names_within_spec_rejected():
    with pytest.raises(ValidationError, match="duplicate table"):
        ModelSpec(
            family="uplift", version="3.1.0", measurement_key="x",
            tables=[
                Table(name="t", path="a.b.c1", key="x",
                      metrics=[Metric(name="m1", column="c", dtype="double")]),
                Table(name="t", path="a.b.c2", key="x",
                      metrics=[Metric(name="m2", column="c", dtype="double")]),
            ],
        )


def test_resolve_metric_returns_table_canonical_and_column():
    spec = _sample_spec()
    tbl, canonical, col = spec.resolve_metric("treatment_effect")
    assert (tbl.name, canonical, col) == ("shared", "treatment_effect", "ate")
    tbl, canonical, col = spec.resolve_metric("te")  # alias
    assert (tbl.name, canonical, col) == ("shared", "treatment_effect", "ate")
    tbl, canonical, col = spec.resolve_metric("heterogeneity_score")
    assert (tbl.name, canonical, col) == ("heterogeneity", "cate_variance", "het_score")


def test_resolve_metric_raises_on_unknown():
    spec = _sample_spec()
    with pytest.raises(KeyError, match="unknown metric"):
        spec.resolve_metric("nope")


def test_resolve_metric_raises_on_cross_table_alias_collision():
    with pytest.raises(ValidationError, match="cross-table"):
        ModelSpec(
            family="uplift", version="3.1.0", measurement_key="x",
            tables=[
                Table(name="a", path="a.b.c1", key="x",
                      metrics=[Metric(name="m", column="c", dtype="double",
                                      aliases=["shared_alias"])]),
                Table(name="b", path="a.b.c2", key="x",
                      metrics=[Metric(name="n", column="c", dtype="double",
                                      aliases=["shared_alias"])]),
            ],
        )


def test_spec_models_are_hashable():
    spec = _sample_spec()
    # Being frozen + all-tuple children means a spec can be used as a dict key.
    d = {spec: "v1"}
    assert d[spec] == "v1"
    # And Metric/Table too, since spec caches them.
    m = spec.tables[0].metrics[0]
    hash(m)
