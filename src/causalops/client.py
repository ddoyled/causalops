"""Consumer-facing entry point: RegistryClient."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterable

from causalops.planner import plan_for_spec, plan_for_specs
from causalops.store.base import Registration, SpecStore, Status, StatusEvent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class RegistryClient:
    store: SpecStore
    spark: "SparkSession"

    # ---- discovery ----------------------------------------------------------

    def list_families(self) -> list[str]:
        return self.store.list_families()

    def list_versions(self, family: str) -> list[str]:
        return self.store.list_versions(family)

    def describe(self, family: str, version: str) -> Registration:
        return self.store.get(family, version)

    def history(self, family: str, version: str) -> list[StatusEvent]:
        return self.store.history(family, version)

    # ---- query --------------------------------------------------------------

    def get_results(
        self,
        *,
        family: str,
        metrics: Iterable[str],
        version: str | None = None,
        status: str | list[str] | None = None,
        as_of: datetime | str | None = None,
    ) -> "DataFrame":
        if version is not None and status is not None:
            raise ValueError("pass either `version` or `status`, not both")
        if version is None and status is None:
            raise ValueError("one of `version` or `status` is required")

        if as_of is not None and isinstance(as_of, str):
            as_of = datetime.fromisoformat(as_of)

        if version is not None:
            regs = [self.store.get(family, version)]
        else:
            statuses = (
                [Status(status)] if isinstance(status, str)
                else [Status(s) for s in status]
            )
            regs = self.store.by_status(family, statuses, as_of=as_of)
            if not regs:
                raise LookupError(
                    f"no {family!r} versions match status(es) {status!r}"
                )

        specs = [r.spec for r in regs]
        if len(specs) == 1 and version is not None:
            # Single explicit version: don't attach a `version` column, keep
            # the surface minimal.
            return plan_for_spec(self.spark, specs[0], metrics=metrics)
        return plan_for_specs(self.spark, specs, metrics=metrics)
