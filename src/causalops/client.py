"""Consumer-facing entry point: RegistryClient."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from causalops.planner import plan_for_spec, plan_for_specs
from causalops.store.base import Registration, SpecStore, Status, StatusEvent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class RegistryClient:
    store: SpecStore
    spark: SparkSession

    # ---- discovery ----------------------------------------------------------

    def list_families(self) -> list[str]:
        """Return all registered family names."""
        return self.store.list_families()

    def list_versions(self, family: str) -> list[str]:
        """Return all registered versions for ``family``."""
        return self.store.list_versions(family)

    def describe(self, family: str, version: str) -> Registration:
        """Return the full registration record for one (family, version)."""
        return self.store.get(family, version)

    def history(self, family: str, version: str) -> list[StatusEvent]:
        """Return the status-event log for one (family, version)."""
        return self.store.history(family, version)

    # ---- production windows --------------------------------------------------

    def production_windows(self, family: str) -> list[tuple[str, str, str | None]]:
        """Return ``[(version, start_iso, end_iso_or_None), ...]`` for each production stint.

        A version's production window opens on its ``PRODUCTION`` event and
        closes on the subsequent ``RETIRED`` event (or stays open if never
        retired).
        """
        windows: list[tuple[str, str, str | None]] = []
        for version in self.store.list_versions(family):
            events = sorted(self.store.history(family, version), key=lambda e: e.effective_from)
            prod_start = None
            for e in events:
                if e.status == Status.PRODUCTION and prod_start is None:
                    prod_start = e.effective_from
                elif e.status == Status.RETIRED and prod_start is not None:
                    windows.append(
                        (
                            version,
                            prod_start.date().isoformat(),
                            e.effective_from.date().isoformat(),
                        )
                    )
                    prod_start = None
            if prod_start is not None:
                windows.append((version, prod_start.date().isoformat(), None))
        return windows

    # ---- query --------------------------------------------------------------

    def get_results(
        self,
        *,
        family: str,
        metrics: Iterable[str],
        version: str | None = None,
        versions: list[str] | None = None,
        status: str | list[str] | None = None,
        as_of: datetime | str | None = None,
    ) -> DataFrame:
        """Query metric results for a family, resolved to canonical column names.

        Select versions by explicit ``version``, a list of ``versions``, or by
        ``status`` (mutually exclusive). When multiple versions match, rows are
        unioned with a ``version`` tag.
        ``as_of`` narrows status lookups to a point in time.
        """
        selectors = sum(x is not None for x in (version, versions, status))
        if selectors > 1:
            raise ValueError("pass exactly one of `version`, `versions`, or `status`")
        if selectors == 0:
            raise ValueError("one of `version`, `versions`, or `status` is required")

        if as_of is not None and isinstance(as_of, str):
            as_of = datetime.fromisoformat(as_of)

        if version is not None:
            regs = [self.store.get(family, version)]
        elif versions is not None:
            regs = [self.store.get(family, v) for v in versions]
        else:
            assert status is not None  # narrowed by the guards above
            statuses = [Status(status)] if isinstance(status, str) else [Status(s) for s in status]
            regs = self.store.by_status(family, statuses, as_of=as_of)
            if not regs:
                raise LookupError(f"no {family!r} versions match status(es) {status!r}")

        specs = [r.spec for r in regs]
        if len(specs) == 1 and version is not None:
            # Single explicit version: don't attach a `version` column, keep
            # the surface minimal.
            return plan_for_spec(self.spark, specs[0], metrics=metrics)
        return plan_for_specs(self.spark, specs, metrics=metrics)
