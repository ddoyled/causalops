"""JSON-file-backed SpecStore.

One JSON file holds both tables:

    {
      "registrations": [ {family, version, spec_json, git_*, sdk_version,
                          registered_by, registered_at}, ... ],
      "status_log":    [ {event_id, family, version, status,
                          effective_from, assigned_by, note}, ... ]
    }

Writes are single-writer: read the whole file, mutate in memory, then
`os.replace` a temp file into place. That's atomic on POSIX and Windows and
gives us the same "one commit contains both prod-swap events" guarantee the
Delta store provided, without needing a live Spark session.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from causalops.spec import ModelSpec
from causalops.store.base import (
    Registration,
    SpecStore,
    Status,
    StatusEvent,
)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.isoformat()


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _new_status_event(family, version, status, assigned_by, note, at):
    return {
        "event_id": str(uuid.uuid4()),
        "family": family,
        "version": version,
        "status": status.value,
        "effective_from": _iso(at),
        "assigned_by": assigned_by,
        "note": note,
    }


@dataclass
class JsonFileSpecStore(SpecStore):
    path: Path

    def __post_init__(self) -> None:
        self.path = Path(self.path)

    # --- file I/O ------------------------------------------------------------

    def _load(self) -> dict[str, list[dict[str, Any]]]:
        if not self.path.exists():
            return {"registrations": [], "status_log": []}
        with self.path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("registrations", [])
        data.setdefault("status_log", [])
        return data

    def _dump(self, data: dict[str, list[dict[str, Any]]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Write to a sibling temp file in the same directory so os.replace is
        # atomic (cross-filesystem renames are not).
        fd, tmp = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=False)
            os.replace(tmp, self.path)
        except Exception:
            # Best-effort cleanup of the temp file if the rename failed.
            Path(tmp).unlink(missing_ok=True)
            raise

    # --- row -> object helpers ----------------------------------------------

    def _row_to_registration(self, r: dict[str, Any]) -> Registration:
        return Registration(
            spec=ModelSpec.model_validate_json(r["spec_json"]),
            git_repo=r["git_repo"],
            git_tag=r["git_tag"],
            git_sha=r["git_sha"],
            sdk_version=r["sdk_version"],
            registered_by=r["registered_by"],
            registered_at=_parse_iso(r["registered_at"]),
        )

    def _row_to_event(self, r: dict[str, Any]) -> StatusEvent:
        return StatusEvent(
            event_id=r["event_id"],
            family=r["family"],
            version=r["version"],
            status=Status(r["status"]),
            effective_from=_parse_iso(r["effective_from"]),
            assigned_by=r["assigned_by"],
            note=r["note"] or "",
        )

    # --- registrations -------------------------------------------------------

    def put(self, spec, *, git_repo, git_tag, git_sha, registered_by, sdk_version):
        data = self._load()
        for r in data["registrations"]:
            if r["family"] == spec.family and r["version"] == spec.version:
                raise KeyError(f"{spec.family}@{spec.version} already registered")
        now = datetime.now(UTC)
        data["registrations"].append(
            {
                "family": spec.family,
                "version": spec.version,
                "spec_json": spec.model_dump_json(),
                "git_repo": git_repo,
                "git_tag": git_tag,
                "git_sha": git_sha,
                "sdk_version": sdk_version,
                "registered_by": registered_by,
                "registered_at": _iso(now),
            }
        )
        data["status_log"].append(
            _new_status_event(
                spec.family,
                spec.version,
                Status.EXPERIMENT,
                registered_by,
                "initial registration",
                now,
            )
        )
        self._dump(data)
        return Registration(
            spec=spec,
            git_repo=git_repo,
            git_tag=git_tag,
            git_sha=git_sha,
            sdk_version=sdk_version,
            registered_by=registered_by,
            registered_at=now,
        )

    def exists(self, family, version):
        for r in self._load()["registrations"]:
            if r["family"] == family and r["version"] == version:
                return True
        return False

    def get(self, family, version):
        for r in self._load()["registrations"]:
            if r["family"] == family and r["version"] == version:
                return self._row_to_registration(r)
        raise KeyError(f"{family}@{version} not found")

    def list_families(self):
        return sorted({r["family"] for r in self._load()["registrations"]})

    def list_versions(self, family):
        return sorted(
            {r["version"] for r in self._load()["registrations"] if r["family"] == family}
        )

    def delete(self, family, version):
        """Remove a registration (used by CLI --force). Not on the ABC.

        Leaves the status_log intact so history() still reflects what happened.
        """
        data = self._load()
        before = len(data["registrations"])
        data["registrations"] = [
            r
            for r in data["registrations"]
            if not (r["family"] == family and r["version"] == version)
        ]
        if len(data["registrations"]) != before:
            self._dump(data)

    # --- status --------------------------------------------------------------

    def promote(self, family, version, status, *, assigned_by, note="", reactivate=False):
        data = self._load()
        if not any(
            r["family"] == family and r["version"] == version for r in data["registrations"]
        ):
            raise KeyError(f"{family}@{version} is not registered")

        current = self._current_status_from(data, family, version)
        if current == Status.RETIRED and not reactivate:
            raise ValueError(f"{family}@{version} is retired; pass reactivate=True to un-retire")

        now = datetime.now(UTC)
        new_events = [_new_status_event(family, version, status, assigned_by, note, now)]

        if status == Status.PRODUCTION:
            # Atomic production swap: any current prod in the family must be
            # retired in the same commit as the new prod's promotion.
            for other in self._by_status_from(data, family, [Status.PRODUCTION]):
                if other["version"] == version:
                    continue
                new_events.append(
                    _new_status_event(
                        family,
                        other["version"],
                        Status.RETIRED,
                        assigned_by,
                        f"auto-retired on promotion of {version} to production",
                        now,
                    )
                )

        data["status_log"].extend(new_events)
        self._dump(data)

    def current_status(self, family, version, *, as_of=None):
        return self._current_status_from(self._load(), family, version, as_of=as_of)

    def by_status(self, family, status, *, as_of=None):
        wanted: list[Status] = (
            [status] if isinstance(status, Status) else [Status(s) for s in status]
        )
        data = self._load()
        rows = self._by_status_from(data, family, wanted, as_of=as_of)
        # Order by version so callers get a deterministic result.
        rows.sort(key=lambda r: r["version"])
        return [
            self._row_to_registration(
                next(
                    reg
                    for reg in data["registrations"]
                    if reg["family"] == family and reg["version"] == r["version"]
                )
            )
            for r in rows
        ]

    def history(self, family, version):
        events = [
            e
            for e in self._load()["status_log"]
            if e["family"] == family and e["version"] == version
        ]
        events.sort(key=lambda e: _parse_iso(e["effective_from"]))
        return [self._row_to_event(e) for e in events]

    # --- internal query helpers (operate on already-loaded data) ------------

    def _current_status_from(
        self,
        data: dict[str, list[dict[str, Any]]],
        family: str,
        version: str,
        *,
        as_of: datetime | None = None,
    ) -> Status:
        matches = [
            e for e in data["status_log"] if e["family"] == family and e["version"] == version
        ]
        if as_of is not None:
            matches = [e for e in matches if _parse_iso(e["effective_from"]) <= as_of]
        if not matches:
            raise KeyError(f"{family}@{version} has no status events at or before as_of")
        latest = max(matches, key=lambda e: _parse_iso(e["effective_from"]))
        return Status(latest["status"])

    def _by_status_from(
        self,
        data: dict[str, list[dict[str, Any]]],
        family: str,
        wanted: list[Status],
        *,
        as_of: datetime | None = None,
    ) -> list[dict[str, Any]]:
        wanted_set = {s.value for s in wanted}
        out: list[dict[str, Any]] = []
        versions = {r["version"] for r in data["registrations"] if r["family"] == family}
        for v in versions:
            try:
                cur = self._current_status_from(data, family, v, as_of=as_of)
            except KeyError:
                continue
            if cur.value in wanted_set:
                out.append({"family": family, "version": v})
        return out
