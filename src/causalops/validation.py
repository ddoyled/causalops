"""Validate a ModelSpec's declared tables/columns against the live catalog.

Distinguishes two failure modes:
- Table missing: warn (expected on first registration before the pipeline runs)
- Table present but column missing or dtype mismatch: error (real drift)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from causalops.spec import ModelSpec
from causalops.utils import read_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class ValidationReport:
    """Accumulated errors and warnings from validating a spec against live tables."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """True if any hard errors (missing columns, dtype mismatches) were found."""
        return bool(self.errors)

    @property
    def has_warnings(self) -> bool:
        """True if any warnings (e.g. table not yet created) were found."""
        return bool(self.warnings)

    def format(self) -> str:
        """Render all errors and warnings as a human-readable string."""
        parts = []
        if self.errors:
            parts.append("Errors:\n  " + "\n  ".join(self.errors))
        if self.warnings:
            parts.append("Warnings:\n  " + "\n  ".join(self.warnings))
        return "\n".join(parts)

    def format_warnings(self) -> str:
        """Render only warnings as a human-readable string."""
        return "Warnings:\n  " + "\n  ".join(self.warnings)


def validate_against_uc(spec: ModelSpec, *, spark: SparkSession) -> ValidationReport:
    report = ValidationReport()
    for table in spec.tables:
        try:
            actual = {
                f.name: f.dataType.simpleString()
                for f in read_table(spark, table.path).schema.fields
            }
        except Exception:
            report.warnings.append(f"{table.path} does not exist yet (first registration?)")
            continue
        for m in table.metrics:
            if m.column not in actual:
                report.errors.append(
                    f"{table.path} missing column {m.column!r} (declared for metric {m.name!r})"
                )
            elif actual[m.column] != m.dtype:
                report.errors.append(
                    f"{table.path}.{m.column}: spec says {m.dtype}, table has {actual[m.column]}"
                )
    return report
