"""Tests for the SpecStore ABC + Registration dataclass surface."""
import inspect
from datetime import datetime

import pytest

from registry_sdk import Metric, ModelSpec, Table
from registry_sdk.store.base import Registration, SpecStore, Status


def test_status_values():
    assert set(s.value for s in Status) == {
        "experiment", "challenger", "production", "retired"
    }


def test_registration_dataclass_roundtrips_datetime():
    spec = ModelSpec(
        family="f", version="1.0.0", measurement_key="k",
        tables=[Table(name="t", path="db.tbl", key="k",
                      metrics=[Metric(name="m", column="c", dtype="double")])],
    )
    reg = Registration(
        spec=spec, git_repo="org/repo", git_tag="v1.0.0",
        git_sha="deadbeef", sdk_version="0.1.0",
        registered_by="alice", registered_at=datetime(2026, 1, 1),
    )
    assert reg.family == "f"
    assert reg.version == "1.0.0"


def test_specstore_is_abstract_and_declares_required_methods():
    assert inspect.isabstract(SpecStore)
    for name in (
        "put", "exists", "get", "list_families", "list_versions",
        "promote", "current_status", "by_status", "history",
    ):
        assert hasattr(SpecStore, name), f"SpecStore missing method {name!r}"
