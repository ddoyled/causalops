"""Delta-backed SpecStore.

Uses the PySpark DataFrame API and Delta's Python builders for all
reads/writes. SQL is used only for `CREATE DATABASE` (no DataFrame
equivalent). Local dev uses embedded-Derby Hive; on Databricks the same code
targets Unity Catalog by pointing `database` at a UC schema (e.g.
`main.registry`).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from causalops.spec import ModelSpec
from causalops.store.base import (
    Registration,
    SpecStore,
    Status,
    StatusEvent,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# Explicit write schemas — createDataFrame infers types from the tuples,
# but we want TIMESTAMP (not string) and non-nullable columns.
_REG_SCHEMA = StructType([
    StructField("family", StringType(), nullable=False),
    StructField("version", StringType(), nullable=False),
    StructField("spec_json", StringType(), nullable=False),
    StructField("git_repo", StringType(), nullable=False),
    StructField("git_tag", StringType(), nullable=False),
    StructField("git_sha", StringType(), nullable=False),
    StructField("sdk_version", StringType(), nullable=False),
    StructField("registered_by", StringType(), nullable=False),
    StructField("registered_at", TimestampType(), nullable=False),
])

_STATUS_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("family", StringType(), nullable=False),
    StructField("version", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("effective_from", TimestampType(), nullable=False),
    StructField("assigned_by", StringType(), nullable=False),
    StructField("note", StringType(), nullable=False),
])


def _new_status_event(family, version, status, assigned_by, note, at):
    return (str(uuid.uuid4()), family, version, status.value, at, assigned_by, note)


@dataclass
class SparkHiveSpecStore(SpecStore):
    spark: "SparkSession"
    database: str

    def ensure_tables(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        (
            DeltaTable.createIfNotExists(self.spark)
            .tableName(f"{self.database}.registrations")
            .addColumn("family", "STRING")
            .addColumn("version", "STRING")
            .addColumn("spec_json", "STRING")
            .addColumn("git_repo", "STRING")
            .addColumn("git_tag", "STRING")
            .addColumn("git_sha", "STRING")
            .addColumn("sdk_version", "STRING")
            .addColumn("registered_by", "STRING")
            .addColumn("registered_at", "TIMESTAMP")
            .execute()
        )
        (
            DeltaTable.createIfNotExists(self.spark)
            .tableName(f"{self.database}.status_log")
            .addColumn("event_id", "STRING")
            .addColumn("family", "STRING")
            .addColumn("version", "STRING")
            .addColumn("status", "STRING")
            .addColumn("effective_from", "TIMESTAMP")
            .addColumn("assigned_by", "STRING")
            .addColumn("note", "STRING")
            .execute()
        )

    # ---- SpecStore abstract methods: stubs; filled in by later tasks --------

    def put(self, spec, *, git_repo, git_tag, git_sha, registered_by, sdk_version):
        if self.exists(spec.family, spec.version):
            raise KeyError(
                f"{spec.family}@{spec.version} already registered"
            )
        now = datetime.now(timezone.utc)
        reg_row = (
            spec.family, spec.version, spec.model_dump_json(),
            git_repo, git_tag, git_sha, sdk_version, registered_by, now,
        )
        (
            self.spark.createDataFrame([reg_row], schema=_REG_SCHEMA)
            .write.format("delta").mode("append")
            .saveAsTable(f"{self.database}.registrations")
        )
        # Initial status event: experiment.
        self._append_status_events([
            _new_status_event(spec.family, spec.version, Status.EXPERIMENT,
                              registered_by, "initial registration", now),
        ])
        return Registration(
            spec=spec, git_repo=git_repo, git_tag=git_tag, git_sha=git_sha,
            sdk_version=sdk_version, registered_by=registered_by,
            registered_at=now,
        )

    def exists(self, family, version):
        return (
            self.spark.table(f"{self.database}.registrations")
            .filter((F.col("family") == family) & (F.col("version") == version))
            .limit(1).take(1)
        ) != []

    def get(self, family, version):
        rows = (
            self.spark.table(f"{self.database}.registrations")
            .filter((F.col("family") == family) & (F.col("version") == version))
            .collect()
        )
        if not rows:
            raise KeyError(f"{family}@{version} not found")
        r = rows[0]
        spec = ModelSpec.model_validate_json(r["spec_json"])
        return Registration(
            spec=spec, git_repo=r["git_repo"], git_tag=r["git_tag"],
            git_sha=r["git_sha"], sdk_version=r["sdk_version"],
            registered_by=r["registered_by"], registered_at=r["registered_at"],
        )

    def list_families(self):
        rows = (
            self.spark.table(f"{self.database}.registrations")
            .select("family").distinct().orderBy("family")
            .collect()
        )
        return [r["family"] for r in rows]

    def list_versions(self, family):
        rows = (
            self.spark.table(f"{self.database}.registrations")
            .filter(F.col("family") == family)
            .select("version").distinct().orderBy("version")
            .collect()
        )
        return [r["version"] for r in rows]

    def _append_status_events(self, rows):
        df = self.spark.createDataFrame(list(rows), schema=_STATUS_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(
            f"{self.database}.status_log"
        )

    def promote(self, family, version, status, *, assigned_by, note="", reactivate=False):
        if not self.exists(family, version):
            raise KeyError(f"{family}@{version} is not registered")

        current = self.current_status(family, version)
        if current == Status.RETIRED and not reactivate:
            raise ValueError(
                f"{family}@{version} is retired; pass reactivate=True to un-retire"
            )

        now = datetime.now(timezone.utc)
        events = [
            _new_status_event(family, version, status, assigned_by, note, now),
        ]

        # Atomic production swap: any current prod in the family must be
        # retired in the same commit as the new prod's promotion.
        if status == Status.PRODUCTION:
            for reg in self.by_status(family, Status.PRODUCTION):
                if reg.version == version:
                    continue
                events.append(
                    _new_status_event(
                        family, reg.version, Status.RETIRED, assigned_by,
                        f"auto-retired on promotion of {version} to production",
                        now,
                    )
                )
        self._append_status_events(events)

    def current_status(self, family, version, *, as_of=None):
        df = (
            self.spark.table(f"{self.database}.status_log")
            .filter((F.col("family") == family) & (F.col("version") == version))
        )
        if as_of is not None:
            df = df.filter(F.col("effective_from") <= F.lit(as_of))
        rows = df.orderBy(F.col("effective_from").desc()).limit(1).collect()
        if not rows:
            raise KeyError(f"{family}@{version} has no status events at or before as_of")
        return Status(rows[0]["status"])

    def by_status(self, family, status, *, as_of=None):
        wanted = [status] if isinstance(status, Status) else list(status)
        wanted_set = {s.value for s in wanted}
        # Get all (family, version) with current status matching.
        out = []
        for v in self.list_versions(family):
            try:
                cur = self.current_status(family, v, as_of=as_of)
            except KeyError:
                continue
            if cur.value in wanted_set:
                out.append(self.get(family, v))
        return out

    def history(self, family, version):
        rows = (
            self.spark.table(f"{self.database}.status_log")
            .filter((F.col("family") == family) & (F.col("version") == version))
            .orderBy(F.col("effective_from").asc())
            .collect()
        )
        return [
            StatusEvent(
                event_id=r["event_id"], family=r["family"], version=r["version"],
                status=Status(r["status"]), effective_from=r["effective_from"],
                assigned_by=r["assigned_by"], note=r["note"] or "",
            )
            for r in rows
        ]
