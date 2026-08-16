"""Registry storage backends and factory."""

from __future__ import annotations

import json
import os

from causalops.paths import default_registry_json_path
from causalops.store.base import Registration, SpecStore, Status, StatusEvent
from causalops.store.json_file import JsonFileSpecStore

__all__ = [
    "Registration",
    "SpecStore",
    "Status",
    "StatusEvent",
    "JsonFileSpecStore",
    "get_store",
]


def get_store() -> SpecStore:
    """Build the configured SpecStore.

    Reads `CAUSALOPS_STORE_CONFIG` (JSON) from the environment, e.g.

        {"backend": "json_file", "path": ".causalops/registry.json"}

    Defaults to a `json_file` backend at `<repo>/.causalops/registry.json`.
    """
    raw = os.environ.get("CAUSALOPS_STORE_CONFIG")
    if raw:
        cfg = json.loads(raw)
    else:
        cfg = {"backend": "json_file", "path": str(default_registry_json_path())}
    backend = cfg.get("backend", "json_file")
    if backend == "json_file":
        return JsonFileSpecStore(path=cfg["path"])
    raise ValueError(f"unsupported backend {backend!r}")
