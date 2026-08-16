"""Tests for RegistryClient (consumer-facing API)."""

from pathlib import Path

import pandas as pd
import pytest

from causalops import Metric, ModelSpec, RegistryClient, Table
from causalops.store.base import Status
from causalops.store.json_file import JsonFileSpecStore


def _write(pdf: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(path, engine="pyarrow", index=False)
    return path


def _seed_result_tables(tmp_path: Path) -> dict[str, Path]:
    return {
        "shared_v3": _write(
            pd.DataFrame({"experiment_id": ["e1", "e2"], "ate": [0.10, 0.20]}),
            tmp_path / "shared_v3.parquet",
        ),
        "shared_v2": _write(
            pd.DataFrame({"experiment_id": ["e1", "e2"], "te": [0.09, 0.19]}),
            tmp_path / "shared_v2.parquet",
        ),
    }


def _spec(family: str, version: str, path: Path, phys: str) -> ModelSpec:
    return ModelSpec(
        family=family,
        version=version,
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path=str(path),
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column=phys, dtype="double", aliases=["te"]),
                ],
            )
        ],
    )


def _seed_registry(tmp_path: Path, paths: dict[str, Path]) -> JsonFileSpecStore:
    store = JsonFileSpecStore(path=tmp_path / "registry.json")
    for spec in [
        _spec("uplift", "2.9.0", paths["shared_v2"], "te"),
        _spec("uplift", "3.0.0", paths["shared_v3"], "ate"),
    ]:
        store.put(
            spec,
            git_repo="org/uplift-model",
            git_tag=f"v{spec.version}",
            git_sha="a" * 40,
            registered_by="alice",
            sdk_version="0.1.0",
        )
    return store


def test_get_results_by_explicit_version(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    store = _seed_registry(tmp_path, paths)
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(family="uplift", version="3.0.0", metrics=["treatment_effect"])
    assert {"experiment_id", "treatment_effect"} <= set(df.columns)
    rows = {r["experiment_id"]: r["treatment_effect"] for r in df.collect()}
    assert rows == {"e1": 0.10, "e2": 0.20}


def test_get_results_by_status_production_returns_one_version(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    store = _seed_registry(tmp_path, paths)
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(family="uplift", status="production", metrics=["treatment_effect"])
    rows = df.collect()
    assert all(r["version"] == "3.0.0" for r in rows)


def test_get_results_multi_status_unions(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    store = _seed_registry(tmp_path, paths)
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "2.9.0", Status.CHALLENGER, assigned_by="bob")
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(
        family="uplift",
        status=["production", "challenger"],
        metrics=["treatment_effect"],
    )
    versions = {r["version"] for r in df.collect()}
    assert versions == {"3.0.0", "2.9.0"}


def test_get_results_rejects_status_and_version_together(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    store = _seed_registry(tmp_path, paths)
    client = RegistryClient(store=store, spark=spark)
    with pytest.raises(ValueError, match="either"):
        client.get_results(
            family="uplift", version="3.0.0", status="production", metrics=["treatment_effect"]
        )


def test_discovery_wrappers(spark, tmp_path):
    paths = _seed_result_tables(tmp_path)
    store = _seed_registry(tmp_path, paths)
    client = RegistryClient(store=store, spark=spark)
    assert client.list_families() == ["uplift"]
    assert sorted(client.list_versions("uplift")) == ["2.9.0", "3.0.0"]
    described = client.describe("uplift", "3.0.0")
    assert described.spec.version == "3.0.0"
    hist = client.history("uplift", "3.0.0")
    assert len(hist) == 1 and hist[0].status == Status.EXPERIMENT
