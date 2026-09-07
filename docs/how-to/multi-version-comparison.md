# Champion / Challenger Comparison

Query champion and challenger results side-by-side to evaluate a new model version before cutting over.

## Pulling two versions by version string

Pass a list of version strings to `get_results()` to union results across the champion and challenger:

```python
from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

spark = build_local_spark_session()
client = RegistryClient(store=get_store(), spark=spark)

CHAMPION = "1.0.0"
CHALLENGER = "1.1.0"

df = client.get_results(
    family="uplift",
    versions=[CHAMPION, CHALLENGER],
    metrics=["treatment_effect", "ci_lower", "ci_upper"],
)
df.show()
```

The returned DataFrame includes a `version` column tagging which version each row came from:

```
+--------------+----------------+----------+----------+-------+
|experiment_id |treatment_effect|ci_lower  |ci_upper  |version|
+--------------+----------------+----------+----------+-------+
|exp_001       |0.042           |0.028     |0.056     |1.0.0  |
|exp_001       |0.039           |0.025     |0.053     |1.1.0  |
|...           |...             |...       |...       |...    |
+--------------+----------------+----------+----------+-------+
```

You can also query by status when you don't know the exact version strings:

```python
df = client.get_results(
    family="uplift",
    status=["production", "challenger"],
    metrics=["treatment_effect", "ci_lower", "ci_upper"],
)
```

!!! warning "Missing metrics"

    When the challenger introduces metrics that the champion doesn't have, the missing columns are filled with `NULL` rather than raising an error:

    ```python
    df = client.get_results(
        family="uplift",
        versions=[CHAMPION, CHALLENGER],
        metrics=["treatment_effect", "cate_variance"],
    )
    ```

    If `cate_variance` exists only in v1.1.0 (the challenger), the champion rows will have `NULL` in that column. This is handled by `plan_for_specs` internally.

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

This produces one row per experiment with columns like `1.0.0_treatment_effect` and `1.1.0_treatment_effect`.

## Computing deltas

```python
delta = comparison.withColumn(
    "te_delta",
    F.col(f"`{CHALLENGER}_treatment_effect`") - F.col(f"`{CHAMPION}_treatment_effect`"),
).select(
    "experiment_id",
    F.col(f"`{CHAMPION}_treatment_effect`").alias("champion_te"),
    F.col(f"`{CHALLENGER}_treatment_effect`").alias("challenger_te"),
    "te_delta",
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

!!! example "Full example"
    See [`examples/champion_challenger.py`](../../examples/champion_challenger.py) for a
    runnable script that pulls both versions, pivots, computes deltas, and prints
    summary statistics.
