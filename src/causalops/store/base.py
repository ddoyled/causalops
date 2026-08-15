"""Storage abstraction for the model registry.

Two conceptual tables:
- `registrations`: immutable, one row per (family, version).
- `status_log`: append-only status events per (family, version).

Concrete implementations must guarantee that promote() writes are atomic;
in particular, a production promotion writes both the incoming version's
`production` event and the outgoing version's `retired` event in one commit.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from causalops.spec import ModelSpec


class Status(StrEnum):
    EXPERIMENT = "experiment"
    CHALLENGER = "challenger"
    PRODUCTION = "production"
    RETIRED = "retired"


@dataclass(frozen=True)
class Registration:
    spec: ModelSpec
    git_repo: str
    git_tag: str
    git_sha: str
    sdk_version: str
    registered_by: str
    registered_at: datetime

    @property
    def family(self) -> str:
        return self.spec.family

    @property
    def version(self) -> str:
        return self.spec.version


@dataclass(frozen=True)
class StatusEvent:
    event_id: str
    family: str
    version: str
    status: Status
    effective_from: datetime
    assigned_by: str
    note: str


class SpecStore(ABC):
    """Two-table registry storage."""

    # --- registrations -------------------------------------------------------

    @abstractmethod
    def put(
        self,
        spec: ModelSpec,
        *,
        git_repo: str,
        git_tag: str,
        git_sha: str,
        registered_by: str,
        sdk_version: str,
    ) -> Registration:
        """Insert a registration and its initial `experiment` status event.

        Raises `KeyError` if (family, version) already exists (callers use
        `exists` first when they want to expose a --force flag)."""

    @abstractmethod
    def exists(self, family: str, version: str) -> bool: ...

    @abstractmethod
    def get(self, family: str, version: str) -> Registration: ...

    @abstractmethod
    def list_families(self) -> list[str]: ...

    @abstractmethod
    def list_versions(self, family: str) -> list[str]: ...

    # --- status --------------------------------------------------------------

    @abstractmethod
    def promote(
        self,
        family: str,
        version: str,
        status: Status,
        *,
        assigned_by: str,
        note: str = "",
        reactivate: bool = False,
    ) -> None:
        """Append a status event. Enforces:

        - `production` promotion is atomic with retirement of the current prod.
        - Un-retiring (retired -> anything) requires `reactivate=True`.
        """

    @abstractmethod
    def current_status(
        self,
        family: str,
        version: str,
        *,
        as_of: datetime | None = None,
    ) -> Status: ...

    @abstractmethod
    def by_status(
        self,
        family: str,
        status: Status | list[Status],
        *,
        as_of: datetime | None = None,
    ) -> list[Registration]: ...

    @abstractmethod
    def history(self, family: str, version: str) -> list[StatusEvent]: ...
