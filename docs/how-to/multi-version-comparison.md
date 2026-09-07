# Compare Metrics Across Versions

Query production and challenger results side-by-side to evaluate a new model version before cutting over.

## Multi-status query

Pass a list of statuses to `get_results()` to union results across all matching versions:

```python
from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

spark = build_local_spark_session()
client = RegistryClient(store=get_store(), spark=spark)

df = client.get_results(
    family="uplift",
    status=["production", "challenger"],
    metrics=["treatment_effect", "ci_lower", "ci_upper"],
)
df.show()
```

The returned DataFrame includes a `version` column tagging which version each row came from:

```
+--------------+----------------+----------+----------+-------+
|experiment_id |treatment_effect|ci_lower  |ci_upper  |version|
+--------------+----------------+----------+----------+-------+
|exp_001       |0.042           |0.028     |0.056     |3.0.0  |
|exp_001       |0.039           |0.025     |0.053     |3.1.0  |
|...           |...             |...       |...       |...    |
+--------------+----------------+----------+----------+-------+
```

## How NULL padding works

When a newer version introduces metrics that the older version doesn't have, the missing columns are filled with `NULL` rather than raising an error:

```python
df = client.get_results(
    family="uplift",
    status=["production", "challenger"],
    metrics=["treatment_effect", "cate_variance"],
)
```

If `cate_variance` exists only in v3.1.0 (challenger), the production rows will have `NULL` in that column. This is handled by `plan_for_specs` internally.

## Side-by-side pivot

To reshape the union into a side-by-side comparison, pivot on the `version` column:

```python
from pyspark.sql import functions as F

comparison = (
    df.groupBy("experiment_id")
    .pivot("version")
    .agg(
        F.first("treatment_effect").alias("treatment_effect"),
        F.first("ci_lower").alias("ci_lower"),
        F.first("ci_upper").alias("ci_upper"),
    )
)
comparison.show()
```

This produces one row per experiment with columns like `3.0.0_treatment_effect` and `3.1.0_treatment_effect`.

## Computing deltas

```python
versions = [row.version for row in df.select("version").distinct().collect()]
prod_v, chall_v = sorted(versions)

delta = (
    comparison
    .withColumn(
        "te_delta",
        F.col(f"`{chall_v}_treatment_effect`") - F.col(f"`{prod_v}_treatment_effect`"),
    )
    .select("experiment_id", f"`{prod_v}_treatment_effect`", f"`{chall_v}_treatment_effect`", "te_delta")
)
delta.show()
```

!!! tip
    Use `as_of` to compare what was in production at a specific point in time:

    ```python
    df = client.get_results(
        family="uplift",
        status="production",
        metrics=["treatment_effect"],
        as_of="2026-08-01",
    )
    ```
