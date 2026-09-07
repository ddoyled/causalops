# Build a Production-of-Record Table

Collect each model family's production results into a single DataFrame, respecting the date windows during which each version held production status.

## The pattern

For each model family:

1. Call `client.production_windows(family)` to get `(version, start_date, end_date)` tuples.
2. For each window, use `plan_for_spec()` to pull that version's results within its date range.
3. Tag rows with `family` and `version`, then union across families.

## Setup

```python
from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

spark = build_local_spark_session()
client = RegistryClient(store=get_store(), spark=spark)
```

## Collecting results for one family

```python
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from causalops.planner import plan_for_spec


def collect_production_results(
    client: RegistryClient,
    family: str,
    metrics: list[str],
) -> DataFrame:
    """Union each production window's rows, aliased to canonical metric names."""
    parts: list[DataFrame] = []
    for version, start, end in client.production_windows(family):
        spec = client.describe(family, version).spec
        df = (
            plan_for_spec(
                client.spark, spec,
                metrics=metrics,
                include_columns=["run_date", "channel_id"],
            )
            .filter(F.col("run_date") >= start)
            .withColumn("family", F.lit(family))
            .withColumn("version", F.lit(version))
        )
        if end is not None:
            df = df.filter(F.col("run_date") < end)
        parts.append(df)
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)
```

Key details:

- `include_columns=["run_date", "channel_id"]` passes through physical columns from the first table so you can filter and partition by them.
- Open windows (`end is None`) mean the version is still in production.
- `unionByName(allowMissingColumns=True)` handles cases where different versions expose different metrics.

## Combining across families

```python
OVERLAPPING_METRICS = [
    "target_total",
    "target_incremental",
    "target_incremental_pct",
    "target_ci_hi",
    "target_ci_lo",
]

FAMILIES = ["scm", "bsts"]

parts = [collect_production_results(client, f, OVERLAPPING_METRICS) for f in FAMILIES]
combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)

combined.orderBy("family", "channel_id", "run_date").show(truncate=False)
```

!!! tip
    Choose `OVERLAPPING_METRICS` to include only the canonical names shared across families. Family-specific metrics (like `seasonality_impact_mean` in BSTS) can be queried separately per family.

## Running the full example

The repository includes a complete working version of this pattern:

```bash
python scripts/seed_examples.py
python examples/keystone.py
```

See [`examples/keystone.py`](https://github.com/your-org/causalops/blob/main/examples/keystone.py) for the source.
