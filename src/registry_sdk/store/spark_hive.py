"""Delta-backed SpecStore.

Uses the PySpark DataFrame API and Delta's Python builders for all
reads/writes. SQL is used only for `CREATE DATABASE` (no DataFrame
equivalent). Local dev uses embedded-Derby Hive; on Databricks the same code
targets Unity Catalog by pointing `database` at a UC schema (e.g.
`main.registry`).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from delta.tables import DeltaTable

from registry_sdk.store.base import (
    Registration,
    SpecStore,
    Status,
    StatusEvent,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


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
        raise NotImplementedError

    def exists(self, family, version):
        raise NotImplementedError

    def get(self, family, version):
        raise NotImplementedError

    def list_families(self):
        raise NotImplementedError

    def list_versions(self, family):
        raise NotImplementedError

    def promote(self, family, version, status, *, assigned_by, note="", reactivate=False):
        raise NotImplementedError

    def current_status(self, family, version, *, as_of=None):
        raise NotImplementedError

    def by_status(self, family, status, *, as_of=None):
        raise NotImplementedError

    def history(self, family, version):
        raise NotImplementedError
