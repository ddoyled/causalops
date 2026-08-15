"""Pydantic models describing a versioned model result contract."""
from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Delta / Spark canonical dtype names we accept in specs. Kept intentionally
# small; expand as needed.
Dtype = Literal["double", "float", "int", "bigint", "string", "boolean", "timestamp"]

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class Metric(BaseModel):
    """A single metric column exposed under a canonical name.

    - `name` is the consumer-facing canonical name.
    - `column` is the physical column in the result table.
    - `aliases` are former canonical names still queryable for continuity.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    column: str = Field(min_length=1)
    dtype: Dtype
    aliases: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _alias_not_equal_to_name(self) -> "Metric":
        if self.name in self.aliases:
            raise ValueError(f"alias may not equal canonical name {self.name!r}")
        return self


class Table(BaseModel):
    """A physical result table storing one or more metrics."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    path: str = Field(
        min_length=1,
        description="Fully-qualified table identifier, e.g. catalog.schema.table",
    )
    key: str = Field(min_length=1, description="Measurement key column name")
    metrics: tuple[Metric, ...]

    @model_validator(mode="after")
    def _no_duplicate_metric_names_or_alias_collisions(self) -> "Table":
        seen_names: set[str] = set()
        seen_lookup: set[str] = set()  # names + aliases combined
        for m in self.metrics:
            if m.name in seen_names:
                raise ValueError(f"duplicate metric {m.name!r} in table {self.name!r}")
            seen_names.add(m.name)
        # alias/canonical collision detection across metrics in same table
        for m in self.metrics:
            for lookup in (m.name, *m.aliases):
                if lookup in seen_lookup:
                    raise ValueError(
                        f"alias {lookup!r} collides with an existing metric name "
                        f"or alias in table {self.name!r}"
                    )
                seen_lookup.add(lookup)
        return self


class ModelSpec(BaseModel):
    """A versioned contract for one model family's result tables."""

    model_config = ConfigDict(frozen=True)

    family: str = Field(min_length=1)
    version: str
    measurement_key: str = Field(min_length=1)
    tables: tuple[Table, ...]

    @model_validator(mode="after")
    def _version_is_semver(self) -> "ModelSpec":
        if not _SEMVER_RE.match(self.version):
            raise ValueError(
                f"version {self.version!r} must be MAJOR.MINOR.PATCH (e.g. 3.1.0)"
            )
        return self

    @model_validator(mode="after")
    def _no_duplicate_table_names(self) -> "ModelSpec":
        seen: set[str] = set()
        for t in self.tables:
            if t.name in seen:
                raise ValueError(f"duplicate table {t.name!r} in spec")
            seen.add(t.name)
        return self

    @model_validator(mode="after")
    def _no_cross_table_alias_collisions(self) -> "ModelSpec":
        """A metric lookup name (canonical or alias) must be unique across the spec."""
        seen: dict[str, str] = {}  # lookup -> table name
        for t in self.tables:
            for m in t.metrics:
                for lookup in (m.name, *m.aliases):
                    if lookup in seen and seen[lookup] != t.name:
                        raise ValueError(
                            f"cross-table collision: metric lookup {lookup!r} "
                            f"appears in tables {seen[lookup]!r} and {t.name!r}"
                        )
                    seen[lookup] = t.name
        return self

    def resolve_metric(self, lookup: str) -> tuple[Table, str, str]:
        """Resolve a canonical name or alias to (Table, canonical_name, physical_column)."""
        for t in self.tables:
            for m in t.metrics:
                if lookup == m.name or lookup in m.aliases:
                    return t, m.name, m.column
        raise KeyError(f"unknown metric {lookup!r} in spec {self.family}@{self.version}")
