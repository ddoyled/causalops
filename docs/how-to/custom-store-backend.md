# Implement a Custom Store Backend

Create a new storage backend by implementing the `SpecStore` abstract base class.

## The SpecStore contract

`SpecStore` defines 9 abstract methods organized into two conceptual tables:

- **Registrations** — immutable, one row per `(family, version)`.
- **Status log** — append-only status events per `(family, version)`.

```python
from causalops.store.base import SpecStore
```

### Registration methods

| Method | Contract |
|--------|---------|
| `put(spec, *, git_repo, git_tag, git_sha, registered_by, sdk_version, registered_at=None)` | Insert a registration and its initial `EXPERIMENT` status event. Raise `KeyError` if `(family, version)` already exists. `registered_at` defaults to `datetime.now(UTC)`. |
| `exists(family, version)` | Return `True` if `(family, version)` is registered. |
| `get(family, version)` | Return the `Registration` record. Raise `KeyError` if not found. |
| `list_families()` | Return all registered family names. |
| `list_versions(family)` | Return all registered versions for a family. |

### Status methods

| Method | Contract |
|--------|---------|
| `promote(family, version, status, *, assigned_by, note="", reactivate=False, effective_from=None)` | Append a status event. See atomicity requirements below. |
| `current_status(family, version, *, as_of=None)` | Return the current status. When `as_of` is set, return the status at that point in time. |
| `by_status(family, status, *, as_of=None)` | Return all registrations matching the given status(es). `status` can be a single `Status` or a list. |
| `history(family, version)` | Return all status events, ordered by `effective_from`. |

## The promote() atomicity invariant

The most critical contract is in `promote()`:

!!! warning "Atomic production swap"
    When promoting a version to `PRODUCTION`, the implementation must atomically retire the current production version. Both the incoming version's `PRODUCTION` event and the outgoing version's `RETIRED` event must share the same `effective_from` timestamp and be committed in a single write.

    This prevents a window where two versions simultaneously claim production status.

Additional `promote()` rules:

- **Reactivate guard**: Promoting a `RETIRED` version to any status requires `reactivate=True`. Without it, raise `ValueError`. This is a safety mechanism — un-retiring silently changes what downstream consumers see.
- **`effective_from`**: Defaults to `datetime.now(UTC)`. Override it for deterministic seed scripts or tests.

## Skeleton implementation

```python
from datetime import datetime, UTC
from causalops.spec import ModelSpec
from causalops.store.base import (
    Registration,
    SpecStore,
    Status,
    StatusEvent,
)


class DeltaSpecStore(SpecStore):
    """SpecStore backed by Delta tables on Unity Catalog."""

    def __init__(self, database: str, spark) -> None:
        self.database = database
        self.spark = spark

    def put(
        self,
        spec: ModelSpec,
        *,
        git_repo: str,
        git_tag: str,
        git_sha: str,
        registered_by: str,
        sdk_version: str,
        registered_at: datetime | None = None,
    ) -> Registration:
        at = registered_at or datetime.now(UTC)
        if self.exists(spec.family, spec.version):
            raise KeyError(f"{spec.family}@{spec.version} already registered")

        # Write to registrations table + initial EXPERIMENT event
        # in a single transaction.
        ...

        return Registration(
            spec=spec,
            git_repo=git_repo,
            git_tag=git_tag,
            git_sha=git_sha,
            sdk_version=sdk_version,
            registered_by=registered_by,
            registered_at=at,
        )

    def exists(self, family: str, version: str) -> bool: ...
    def get(self, family: str, version: str) -> Registration: ...
    def list_families(self) -> list[str]: ...
    def list_versions(self, family: str) -> list[str]: ...

    def promote(
        self,
        family: str,
        version: str,
        status: Status,
        *,
        assigned_by: str,
        note: str = "",
        reactivate: bool = False,
        effective_from: datetime | None = None,
    ) -> None:
        at = effective_from or datetime.now(UTC)
        current = self.current_status(family, version)

        if current == Status.RETIRED and not reactivate:
            raise ValueError(
                f"{family}@{version} is retired; pass reactivate=True to un-retire"
            )

        if status == Status.PRODUCTION:
            # Atomically: write PRODUCTION event for this version
            # AND RETIRED event for the current production version (if any).
            # Both events must share the same effective_from timestamp.
            ...
        else:
            # Write a single status event.
            ...

    def current_status(self, family: str, version: str, *, as_of: datetime | None = None) -> Status: ...
    def by_status(self, family: str, status: Status | list[Status], *, as_of: datetime | None = None) -> list[Registration]: ...
    def history(self, family: str, version: str) -> list[StatusEvent]: ...
```

!!! tip
    Use [`JsonFileSpecStore`][causalops.store.json_file.JsonFileSpecStore] as your reference implementation. It demonstrates all the edge cases: atomic production swaps, the reactivate guard, and point-in-time `as_of` queries.

## Wiring into the factory

The `get_store()` factory reads the `CAUSALOPS_STORE_CONFIG` environment variable (JSON) and dispatches on the `"backend"` key.

To register your backend, add an `elif` branch in `causalops/store/__init__.py`:

```python
def get_store() -> SpecStore:
    raw = os.environ.get("CAUSALOPS_STORE_CONFIG")
    if raw:
        cfg = json.loads(raw)
    else:
        cfg = {"backend": "json_file", "path": str(default_registry_json_path())}

    backend = cfg.get("backend", "json_file")
    if backend == "json_file":
        return JsonFileSpecStore(path=cfg["path"])
    elif backend == "delta":
        from your_module import DeltaSpecStore
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        return DeltaSpecStore(database=cfg["database"], spark=spark)
    raise ValueError(f"unsupported backend {backend!r}")
```

Then configure it:

```bash
export CAUSALOPS_STORE_CONFIG='{"backend": "delta", "database": "main.registry"}'
```
