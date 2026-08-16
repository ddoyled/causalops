"""Read a spec-declared table into a Spark DataFrame.

`Table.path` values are interpreted two ways:

- Filesystem paths (contain `/` or a URL scheme) go through
  `spark.read.parquet(path)`. Local dev uses this — the seed script writes
  Parquet dirs under `<repo>/.causalops/data/`, and specs point at them.
- Anything else is treated as a catalog identifier and read via
  `spark.table(path)`. This is the Databricks / Unity Catalog path —
  specs on that platform point at `catalog.schema.table` names.

Keeps the planner and validator identical across the two environments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _looks_like_filesystem_path(path: str) -> bool:
    return "/" in path or "://" in path


def read_result_table(spark: SparkSession, path: str) -> DataFrame:
    if _looks_like_filesystem_path(path):
        return spark.read.parquet(path)
    return spark.table(path)
