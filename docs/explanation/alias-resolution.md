# Alias Resolution

## The problem

When a model team ships a new version, physical column names in the result tables may change. Version 3.0.0 might store the average treatment effect in a column called `ate`, while version 3.1.0 renames it to `treatment_effect_avg` for clarity. If downstream consumers hard-code column names, every schema change breaks their queries.

## Canonical names as the stable API

Each `Metric` in a `ModelSpec` has two names:

- **`name`** (canonical): the consumer-facing name that stays stable across versions.
- **`column`** (physical): the actual column in the result table, which can change.

```python
Metric(name="treatment_effect", column="ate", dtype="double")
```

Consumers always request canonical names:

```python
client.get_results(family="uplift", metrics=["treatment_effect"])
```

The planner resolves `"treatment_effect"` to the physical column `"ate"` through the spec and renames it in the output DataFrame. The consumer never sees `"ate"`.

## Aliases for backwards compatibility

When a canonical name is renamed between versions, the old name becomes an **alias** so existing queries keep working.

Suppose v3.0.0 used the canonical name `"te"`:

```python
# v3.0.0
Metric(name="te", column="ate", dtype="double")
```

In v3.1.0, the team renames it to `"treatment_effect"` but keeps `"te"` as an alias:

```python
# v3.1.0
Metric(name="treatment_effect", column="ate", dtype="double", aliases=["te"])
```

Now both of these resolve correctly:

```python
spec.resolve_metric("te")                # → (shared_table, "treatment_effect", "ate")
spec.resolve_metric("treatment_effect")  # → (shared_table, "treatment_effect", "ate")
```

A consumer using the old name `"te"` gets the same data they always did. A consumer using the new name `"treatment_effect"` also works. The output column is always named after the canonical name (`"treatment_effect"`), regardless of which lookup name was used.

## How resolve_metric works

`ModelSpec.resolve_metric(lookup)` walks all tables and all metrics in the spec:

1. For each metric, check if `lookup` matches the canonical `name`.
2. If not, check if `lookup` appears in the metric's `aliases` tuple.
3. Return `(Table, canonical_name, physical_column)` on the first match.
4. Raise `KeyError` if no metric matches.

The canonical name is checked before aliases, but in practice this ordering doesn't matter because the collision validators (described below) ensure every lookup name is unique across the entire spec.

## Collision detection

Ambiguous lookup names would make `resolve_metric()` non-deterministic, so the spec validators enforce uniqueness at two levels:

**Within a table**: No two metrics can share a canonical name, and no alias can collide with any canonical name or other alias in the same table. This is enforced by `Table._no_duplicate_metric_names_or_alias_collisions`.

**Across tables**: A lookup name (canonical or alias) that appears in one table cannot appear in another table within the same spec. This is enforced by `ModelSpec._no_cross_table_alias_collisions`. Without this rule, `resolve_metric("foo")` could match metrics in two different tables, and the planner wouldn't know which table to read.

Additionally, an alias cannot equal its own metric's canonical name — that would be redundant and signals a likely mistake. This is enforced by `Metric._alias_not_equal_to_name`.

## Cross-version resolution in multi-version queries

When `plan_for_specs` unions results across multiple versions, each version's spec independently resolves metrics. This means:

- v3.0.0 might resolve `"treatment_effect"` to physical column `"ate"`
- v3.1.0 might resolve `"treatment_effect"` to physical column `"treatment_effect_avg"`

Both produce an output column called `"treatment_effect"`, and the union aligns them by name. The `version` column tags which rows came from which spec, so the physical column difference is invisible to the consumer.

If a metric exists in one version but not another, `plan_for_specs` pads the missing version's rows with NULLs rather than failing. See [Planner Internals](planner-internals.md) for details.
