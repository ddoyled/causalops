"""keystone.py — production-collection demo.

Assumes ``python scripts/seed_examples.py`` has already populated the
local warehouse and registry with the shadow-deployment scenarios (one
per family, each on its own channel). This script is a pure consumer.

Shows how a downstream production process would build a
"production-of-record" table by walking the registry:

1. For each family, derive each version's production window(s) from its
   status log — a window opens on ``PRODUCTION`` and closes on
   ``RETIRED``. During shadow overlap, the challenger's rows are
   ignored; the still-in-prod version's rows are what count.
2. For each window, pull that version's results within its ``run_date``
   range and alias the physical metric columns to their canonical names.
3. Union across families, tagging every row with ``family``, ``version``,
   and (carried forward from the parquet) ``channel_id``.

Result: one row per (family, version, channel_id, rid, run_date) with
the overlapping metric columns. Run it from the repo root:

    python examples/keystone.py
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from causalops import RegistryClient
from causalops.spark_session import build_local_spark_session
from causalops.store import Status, get_store
from causalops.store.base import SpecStore

# Canonical metric names both scm and bsts expose (aliased to different
# physical columns in each family's spec).
OVERLAPPING_METRICS = [
    "target_total",
    "target_incremental",
    "target_incremental_pct",
    "target_ci_hi",
    "target_ci_lo",
]

# Families that carry a channel_id dimension. Uplift lives outside this
# demo — it isn't per-channel.
CHANNEL_FAMILIES = ["scm", "bsts"]


def production_windows(store: SpecStore, family: str) -> list[tuple[str, str, str | None]]:
    """Return ``[(version, start_iso, end_iso_or_None), ...]`` for each production stint.

    Walks the family's status log: a version's production window opens
    on its ``PRODUCTION`` event and closes on the subsequent ``RETIRED``
    event (or stays open if never retired). A version can have multiple
    stints if it's re-promoted after retirement.
    """
    windows: list[tuple[str, str, str | None]] = []
    for version in store.list_versions(family):
        events = sorted(store.history(family, version), key=lambda e: e.effective_from)
        prod_start = None
        for e in events:
            if e.status == Status.PRODUCTION and prod_start is None:
                prod_start = e.effective_from
            elif e.status == Status.RETIRED and prod_start is not None:
                windows.append(
                    (version, prod_start.date().isoformat(), e.effective_from.date().isoformat())
                )
                prod_start = None
        if prod_start is not None:
            windows.append((version, prod_start.date().isoformat(), None))
    return windows


def collect_production_results(
    spark: SparkSession,
    store: SpecStore,
    family: str,
    metrics: list[str],
) -> DataFrame:
    """Union each production window's rows for ``family``, aliased to canonical metric names."""
    parts: list[DataFrame] = []
    for version, start, end in production_windows(store, family):
        spec = store.get(family, version).spec
        tables = {spec.resolve_metric(m)[0] for m in metrics}
        if len(tables) != 1:
            raise ValueError(
                f"{family}: metrics {metrics} span multiple tables — this demo assumes a single results table"
            )
        (table,) = tables

        selects = [
            F.col(table.key).alias(spec.measurement_key),
            F.col("run_date"),
            F.col("channel_id"),
        ]
        for m in metrics:
            _, canonical, physical = spec.resolve_metric(m)
            selects.append(F.col(physical).alias(canonical))

        df = (
            spark.read.parquet(table.path)
            .select(*selects)
            .filter(F.col("run_date") >= start)
            .withColumn("family", F.lit(family))
            .withColumn("version", F.lit(version))
        )
        if end is not None:
            df = df.filter(F.col("run_date") < end)
        parts.append(df)
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)


def main() -> None:
    spark = build_local_spark_session()
    store = get_store()
    client = RegistryClient(store=store, spark=spark)

    for family in CHANNEL_FAMILIES:
        if not client.list_versions(family):
            raise SystemExit(
                f"registry has no {family!r} versions — run `python scripts/seed_examples.py` first"
            )

    print("Production windows per family:")
    for family in CHANNEL_FAMILIES:
        for version, start, end in production_windows(store, family):
            print(f"  {family:8s}  v{version}  {start} .. {end or '(open)'}")

    parts = [
        collect_production_results(spark, store, f, OVERLAPPING_METRICS) for f in CHANNEL_FAMILIES
    ]
    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)

    print()
    print(f"Production-of-record table ({combined.count()} rows):")
    combined.orderBy("family", "channel_id", "run_date", "rid").show(20, truncate=False)


if __name__ == "__main__":
    main()
