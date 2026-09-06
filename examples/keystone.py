"""keystone.py — production-collection demo.

Assumes ``python scripts/seed_examples.py`` has already populated the
local warehouse and registry with the shadow-deployment scenarios (one
per family, each on its own channel). This script is a pure consumer.

Shows how a downstream production process would build a
"production-of-record" table by walking the registry:

1. For each family, derive each version's production window(s) via
   ``client.production_windows()`` — a window opens on ``PRODUCTION``
   and closes on ``RETIRED``.
2. For each window, use the planner to pull that version's results
   within its ``run_date`` range, aliased to canonical metric names.
3. Union across families, tagging every row with ``family``, ``version``,
   and (carried forward from the parquet) ``channel_id``.

Result: one row per (family, version, channel_id, rid, run_date) with
the overlapping metric columns. Run it from the repo root:

    python examples/keystone.py
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from causalops import RegistryClient
from causalops.planner import plan_for_spec
from causalops.store import get_store
from causalops.utils import build_local_spark_session

OVERLAPPING_METRICS = [
    "target_total",
    "target_incremental",
    "target_incremental_pct",
    "target_ci_hi",
    "target_ci_lo",
]

CHANNEL_FAMILIES = ["scm", "bsts"]


def collect_production_results(
    client: RegistryClient,
    family: str,
    metrics: list[str],
) -> DataFrame:
    """Union each production window's rows for ``family``, aliased to canonical metric names."""
    parts: list[DataFrame] = []
    for version, start, end in client.production_windows(family):
        spec = client.describe(family, version).spec
        df = (
            plan_for_spec(
                client.spark, spec, metrics=metrics, include_columns=["run_date", "channel_id"]
            )
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
        for version, start, end in client.production_windows(family):
            print(f"  {family:8s}  v{version}  {start} .. {end or '(open)'}")

    parts = [collect_production_results(client, f, OVERLAPPING_METRICS) for f in CHANNEL_FAMILIES]
    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)

    print()
    print(f"Production-of-record table ({combined.count()} rows):")
    combined.orderBy("family", "channel_id", "run_date", "rid").show(20, truncate=False)


if __name__ == "__main__":
    main()
