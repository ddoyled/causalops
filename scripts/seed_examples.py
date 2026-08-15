"""Seed the local Delta warehouse with mock tables for the examples.

Registry-of-generators pattern: each `examples/<name>/` gets an entry in
`GENERATORS`; the entry is a callable that receives a Spark session and writes
its example's tables. Add new examples by writing a `seed_<name>` function
and registering it below.

Usage
-----
    # Seed one example:
    python scripts/seed_examples.py --example uplift-model

    # Seed everything registered:
    python scripts/seed_examples.py --example all

By default this writes into the same warehouse the CLI uses
(`~/.causalops/warehouse`), so subsequent `causalops register` runs see
the tables. Override with `--warehouse-dir` / `--metastore-dir` for a scratch
workspace.

Paths are 2-part (`<db>.<table>`) — matches the current POC's local-Spark
target. If/when we move to Unity Catalog, spec paths become 3-part
(`catalog.db.table`) and the same `_write_delta` call works unchanged (the
first segment just becomes a UC catalog instead of a Hive database).
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import click
from pyspark.sql import SparkSession

from causalops.spark_session import build_local_spark_session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_delta(spark: SparkSession, df, path: str) -> int:
    """Write `df` as a Delta table at `path` (2-part `<db>.<table>`).

    Creates the database if needed. Idempotent: `mode=overwrite` replaces any
    prior seeded content. Returns the row count for the caller's log line.
    """
    db, _ = path.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    df.write.format("delta").mode("overwrite").saveAsTable(path)
    count = spark.table(path).count()
    click.echo(f"  wrote {path} ({count} rows)")
    return count


# ---------------------------------------------------------------------------
# Generators — one per example. Fill in the DataFrames.
# ---------------------------------------------------------------------------

def seed_uplift_model(spark: SparkSession) -> None:
    """Mock tables for examples/uplift-model/model_spec.py.

    Referenced tables (see the spec for the source of truth):

      uplift.shared_v3
        experiment_id : string   (key)
        ate           : double   (metric: treatment_effect / te)
        ci_lo         : double   (metric: ci_lower)
        ci_hi         : double   (metric: ci_upper)

      uplift.het_v3
        experiment_id : string   (key)
        het_score     : double   (metric: cate_variance / heterogeneity_score)
    """
    # TODO: build the shared_v3 DataFrame.
    # Suggested shape:
    #   shared = spark.createDataFrame(
    #       [("exp_001", 0.042, 0.031, 0.053), ...],
    #       schema="experiment_id STRING, ate DOUBLE, ci_lo DOUBLE, ci_hi DOUBLE",
    #   )
    #   _write_delta(spark, shared, "uplift.shared_v3")
    raise NotImplementedError("fill in uplift.shared_v3")

    # TODO: build the het_v3 DataFrame.
    #   het = spark.createDataFrame(
    #       [("exp_001", 0.11), ...],
    #       schema="experiment_id STRING, het_score DOUBLE",
    #   )
    #   _write_delta(spark, het, "uplift.het_v3")


# Add new examples here. Key MUST match the directory name under examples/.
GENERATORS: dict[str, Callable[[SparkSession], None]] = {
    "uplift-model": seed_uplift_model,
    # "other-model": seed_other_model,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--example",
    default="all",
    show_default=True,
    help="Example name (matches examples/<name>/) or 'all'.",
)
@click.option(
    "--warehouse-dir", type=click.Path(path_type=Path),
    default=Path.home() / ".causalops" / "warehouse",
    envvar="REGISTRY_WAREHOUSE_DIR", show_default=True,
)
@click.option(
    "--metastore-dir", type=click.Path(path_type=Path),
    default=Path.home() / ".causalops" / "metastore_db",
    envvar="REGISTRY_METASTORE_DIR", show_default=True,
)
def main(example: str, warehouse_dir: Path, metastore_dir: Path) -> None:
    """Seed the local warehouse with example mock tables."""
    if example != "all" and example not in GENERATORS:
        known = ", ".join(sorted(GENERATORS)) or "(none registered)"
        raise click.ClickException(
            f"unknown example {example!r}. Known: {known}"
        )

    to_run = list(GENERATORS.items()) if example == "all" else [(example, GENERATORS[example])]

    spark = build_local_spark_session(
        warehouse_dir=warehouse_dir,
        metastore_dir=metastore_dir,
        app_name="causalops_seed_examples",
    )
    try:
        for name, generator in to_run:
            click.echo(f"seeding {name} ...")
            generator(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
