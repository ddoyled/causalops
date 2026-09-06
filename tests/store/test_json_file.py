"""Tests for JsonFileSpecStore — covers the same contract as the deprecated
SparkHiveSpecStore did (put/get/exists/list, promote atomicity + reactivate,
current_status/by_status/history, as_of point-in-time)."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from causalops.store.base import Status
from causalops.store.json_file import JsonFileSpecStore
from tests.fixtures import sample_spec


@pytest.fixture
def store(tmp_path):
    return JsonFileSpecStore(path=tmp_path / "registry.json")


def _put(store, spec):
    return store.put(
        spec,
        git_repo="org/uplift-model",
        git_tag=f"v{spec.version}",
        git_sha="a" * 40,
        registered_by="alice",
        sdk_version="0.1.0",
    )


# --- put / exists / get / list ---------------------------------------------


def test_put_writes_registration_and_initial_experiment_event(store):
    spec = sample_spec()
    reg = _put(store, spec)
    assert reg.family == "uplift" and reg.version == "3.1.0"

    data = json.loads(store.path.read_text())
    assert len(data["registrations"]) == 1
    assert data["registrations"][0]["family"] == "uplift"
    assert len(data["status_log"]) == 1
    assert data["status_log"][0]["status"] == Status.EXPERIMENT.value


def test_put_rejects_duplicate_family_version(store):
    _put(store, sample_spec())
    with pytest.raises(KeyError, match="already registered"):
        _put(store, sample_spec())


def test_exists_and_get(store):
    assert not store.exists("uplift", "3.1.0")
    spec = sample_spec()
    _put(store, spec)
    assert store.exists("uplift", "3.1.0")
    fetched = store.get("uplift", "3.1.0")
    assert fetched.spec == spec
    assert fetched.git_repo == "org/uplift-model"
    assert fetched.git_sha == "a" * 40


def test_get_raises_when_missing(store):
    with pytest.raises(KeyError, match="not found"):
        store.get("uplift", "9.9.9")


def test_list_families_and_versions(store):
    _put(store, sample_spec(version="3.0.0"))
    _put(store, sample_spec(version="3.1.0"))
    assert store.list_families() == ["uplift"]
    assert store.list_versions("uplift") == ["3.0.0", "3.1.0"]


# --- promote ----------------------------------------------------------------


def test_promote_to_challenger(store):
    _put(store, sample_spec(version="3.0.0"))
    store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob")
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER


def test_promote_unknown_version_raises(store):
    _put(store, sample_spec(version="3.0.0"))
    with pytest.raises(KeyError, match="not registered"):
        store.promote("uplift", "9.9.9", Status.CHALLENGER, assigned_by="bob")


def test_promote_to_production_retires_current_prod_atomically(store):
    _put(store, sample_spec(version="3.0.0"))
    _put(store, sample_spec(version="3.1.0"))
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "3.1.0", Status.PRODUCTION, assigned_by="bob", note="graduated")
    prods = [r.version for r in store.by_status("uplift", Status.PRODUCTION)]
    assert prods == ["3.1.0"]
    assert store.current_status("uplift", "3.0.0") == Status.RETIRED
    # Same-commit guarantee: retirement and new-prod events share effective_from.
    e_30_retired = [e for e in store.history("uplift", "3.0.0") if e.status == Status.RETIRED]
    e_31_prod = [e for e in store.history("uplift", "3.1.0") if e.status == Status.PRODUCTION]
    assert e_30_retired and e_31_prod
    assert e_30_retired[0].effective_from == e_31_prod[0].effective_from


def test_unretire_requires_reactivate_flag(store):
    _put(store, sample_spec(version="3.0.0"))
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "3.0.0", Status.RETIRED, assigned_by="bob")
    with pytest.raises(ValueError, match="reactivate"):
        store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob")
    store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob", reactivate=True)
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER


# --- status queries ---------------------------------------------------------


def test_current_status_starts_as_experiment(store):
    _put(store, sample_spec(version="3.0.0"))
    assert store.current_status("uplift", "3.0.0") == Status.EXPERIMENT


def test_by_status_returns_matching_registrations(store):
    _put(store, sample_spec(version="3.0.0"))
    _put(store, sample_spec(version="3.1.0"))
    regs = store.by_status("uplift", Status.EXPERIMENT)
    assert [r.version for r in regs] == ["3.0.0", "3.1.0"]
    assert store.by_status("uplift", Status.PRODUCTION) == []


def test_by_status_accepts_list_of_statuses(store):
    _put(store, sample_spec(version="3.0.0"))
    regs = store.by_status("uplift", [Status.EXPERIMENT, Status.CHALLENGER])
    assert len(regs) == 1


def test_history_returns_events_in_order(store):
    _put(store, sample_spec(version="3.0.0"))
    events = store.history("uplift", "3.0.0")
    assert len(events) == 1
    assert events[0].status == Status.EXPERIMENT


def test_current_status_missing_version_raises(store):
    _put(store, sample_spec(version="3.0.0"))
    with pytest.raises(KeyError):
        store.current_status("uplift", "9.9.9")


def test_current_status_respects_as_of(store):
    """Point-in-time query: a future event must not be visible for an earlier as_of."""
    _put(store, sample_spec(version="3.0.0"))
    # Splice a future event straight into the file to keep this deterministic.
    import uuid

    data = json.loads(store.path.read_text())
    data["status_log"].append(
        {
            "event_id": str(uuid.uuid4()),
            "family": "uplift",
            "version": "3.0.0",
            "status": Status.PRODUCTION.value,
            "effective_from": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
            "assigned_by": "alice",
            "note": "",
        }
    )
    store.path.write_text(json.dumps(data))

    before = datetime.now(UTC) - timedelta(days=365)
    with pytest.raises(KeyError):
        store.current_status("uplift", "3.0.0", as_of=before)
    assert store.current_status("uplift", "3.0.0", as_of=datetime.now(UTC)) == Status.EXPERIMENT


# --- misc ------------------------------------------------------------------


def test_new_store_has_no_registrations(store):
    assert store.list_families() == []
    assert store.list_versions("uplift") == []
    assert not store.exists("uplift", "3.0.0")


def test_delete_removes_registration_but_keeps_history(store):
    _put(store, sample_spec(version="3.0.0"))
    store.delete("uplift", "3.0.0")
    assert not store.exists("uplift", "3.0.0")
    # status_log entries remain so history is auditable.
    assert store.history("uplift", "3.0.0")
