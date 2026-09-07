# Query Results

In this tutorial you will use the
[`RegistryClient`][causalops.client.RegistryClient] to discover registered
models and query their results as Spark DataFrames with canonical column names.

**Audience:** Consumer / downstream analytics.

**Prerequisites:** [Promote through the Lifecycle](promote-lifecycle.md)
complete — you have at least one version registered and promoted to production.

---

## Step 1: Build a RegistryClient

```python
from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

spark = build_local_spark_session()
client = RegistryClient(store=get_store(), spark=spark)
```

The client needs two things: a
[`SpecStore`][causalops.store.base.SpecStore] to read registrations and status
events, and a `SparkSession` to read result tables.

## Step 2: Discover what is registered

### List families

```python
client.list_families()
# ['uplift']
```

### List versions

```python
client.list_versions("uplift")
# ['3.0.0', '3.1.0']
```

### Describe a version

[`describe()`][causalops.client.RegistryClient.describe] returns the full
[`Registration`][causalops.store.base.Registration] record:

```python
reg = client.describe("uplift", "3.1.0")
print(reg.git_repo)       # 'local/uplift-model'
print(reg.registered_by)  # 'alice'
print(reg.spec.tables)    # the full spec with tables and metrics
```

### View status history

```python
for event in client.history("uplift", "3.1.0"):
    print(f"{event.status.value:12s}  {event.effective_from}")
```

## Step 3: Query by explicit version

Request specific metrics for a known version:

```python
df = client.get_results(
    family="uplift",
    version="3.1.0",
    metrics=["treatment_effect", "cate_variance"],
)
df.show(5)
```

```
+-------------+----------------+-------------+
|experiment_id|treatment_effect|cate_variance|
+-------------+----------------+-------------+
|      exp_001|          0.0412|       0.1523|
|      exp_002|          0.0387|       0.0891|
|      exp_003|          0.0445|       0.2104|
+-------------+----------------+-------------+
```

The planner resolves each metric to its physical column, reads the
relevant tables, and outer-joins them on the measurement key. The returned
DataFrame uses **canonical names** as column headers, not the physical column
names.

## Step 4: Query by status

Instead of specifying a version, query by lifecycle status:

```python
df = client.get_results(
    family="uplift",
    status="production",
    metrics=["treatment_effect", "ci_lower", "ci_upper"],
)
df.show(5)
```

This returns results for whichever version is currently in production.

!!! note
    You must pass **either** `version` or `status`, never both. Passing both
    raises `ValueError`.

## Step 5: Multi-status union

Query multiple statuses to compare production and challenger side-by-side:

```python
df = client.get_results(
    family="uplift",
    status=["production", "challenger"],
    metrics=["treatment_effect", "cate_variance"],
)
df.show(5)
```

```
+-------------+----------------+-------------+-------+
|experiment_id|treatment_effect|cate_variance|version|
+-------------+----------------+-------------+-------+
|      exp_001|          0.0412|       0.1523|  3.1.0|
|      exp_001|          0.0390|         null|  3.0.0|
+-------------+----------------+-------------+-------+
```

When multiple versions match, the result includes a `version` column to
distinguish rows. Metrics that exist in one version but not the other are
padded with `null`.

## Step 6: Point-in-time queries with as_of

The `as_of` parameter narrows status lookups to a point in time:

```python
from datetime import datetime, timezone

df = client.get_results(
    family="uplift",
    status="production",
    metrics=["treatment_effect"],
    as_of=datetime(2026, 8, 15, tzinfo=timezone.utc),
)
```

This returns whichever version was in production on 2026-08-15. You can also
pass an ISO string:

```python
df = client.get_results(
    family="uplift",
    status="production",
    metrics=["treatment_effect"],
    as_of="2026-08-15T00:00:00+00:00",
)
```

!!! tip
    Point-in-time queries are useful for backfills: if you need to regenerate
    a downstream table for a past date, use `as_of` to get the version that
    was in production at that time.

## Step 7: Alias resolution

Consumers can request metrics by either their canonical name or any alias.
The returned column always uses the **canonical** name:

```python
df = client.get_results(
    family="uplift",
    version="3.1.0",
    metrics=["te"],  # alias for "treatment_effect"
)
print(df.columns)
# ['experiment_id', 'treatment_effect']
```

Requesting `"te"` returns a column named `"treatment_effect"` — the canonical
name. This ensures that downstream code can rely on stable column names even
as aliases are added across versions.

---

You have now completed the full tutorial sequence. For task-oriented recipes,
see the [How-To Guides](../how-to/index.md). For detailed API documentation,
see the [Reference](../reference/index.md).
