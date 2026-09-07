# Planner Internals

The planner translates a list of canonical metric names into a PySpark DataFrame by reading physical tables, renaming columns, and joining or unioning as needed. There are two entry points: `plan_for_spec` (one version) and `plan_for_specs` (multiple versions).

## Single-spec planning: plan_for_spec

Given one `ModelSpec` and a list of requested metric names, `plan_for_spec` produces a joined DataFrame:

1. **Resolve metrics.** For each requested name, call `spec.resolve_metric(name)` to get `(Table, canonical_name, physical_column)`. This groups the requested metrics by the table that owns them.

2. **Read and select per table.** For each table that has at least one requested metric:
    - Read the table via `read_table(spark, table.path)` — Parquet for filesystem paths, `spark.table()` for catalog identifiers.
    - Build a select list: the table's key column aliased to the spec's `measurement_key`, plus each physical column aliased to its canonical name.
    - If `include_columns` were requested (e.g., `run_date`, `channel_id`), attach them from the first table that has them.

3. **Outer-join across tables.** If metrics come from multiple tables, the per-table DataFrames are outer-joined on the `measurement_key`. An outer join ensures rows appear even if one table has entries the other doesn't.

4. **Result.** One row per `measurement_key` value, with columns named after canonical metric names.

### include_columns

The `include_columns` parameter passes through extra physical columns that aren't declared as metrics — typically context columns like `run_date` or `channel_id` that downstream processes need for filtering.

These columns are selected from the **first** table in the join only, not from every table. This avoids ambiguity — if multiple tables have a `run_date` column, including both in a join would create `run_date` and `run_date_1` columns. By picking from one table, the output stays clean.

## Multi-spec planning: plan_for_specs

Given multiple `ModelSpec` objects (typically different versions of the same family), `plan_for_specs` produces a unioned DataFrame tagged with a `version` column:

1. **Plan each spec independently.** For each spec, determine which of the requested metrics it knows about (via `resolve_metric`, catching `KeyError` for unknown ones). Run `plan_for_spec` on just the known metrics.

2. **Tag with version.** Add a `version` column containing `spec.version` as a literal string.

3. **Pad missing metrics with NULL.** For any requested metric that a spec doesn't know about, add a NULL column (`F.lit(None).cast("double")`). This ensures every spec's DataFrame has the same set of columns.

4. **Standardize column order.** Select columns in a consistent order — `measurement_key`, then all requested metrics, then `version` — so the union aligns correctly.

5. **Union.** Combine all per-spec DataFrames with `unionByName(allowMissingColumns=True)`.

### Why NULL padding instead of errors

A newer model version might introduce metrics that older versions don't have. For example, v1.1.0 adds `cate_variance` while v1.0.0 only has `treatment_effect`. If a consumer queries both versions for `["treatment_effect", "cate_variance"]`, the planner:

- Returns `treatment_effect` and `cate_variance` values for v1.1.0 rows
- Returns `treatment_effect` values and NULL `cate_variance` for v1.0.0 rows

This is the right behavior for cross-version comparison. The alternative — raising an error when any version is missing a requested metric — would make it impossible to compare versions with different metric sets, which is exactly when comparison is most valuable (evaluating whether a new metric is worth adding).

## The read_table dual-path

`read_table(spark, path)` in `causalops.utils` inspects the path string to decide how to read:

- If the path contains `/` or `://` → **filesystem path** → `spark.read.parquet(path)`
- Otherwise → **catalog identifier** → `spark.table(path)` (Unity Catalog on Databricks, Hive metastore elsewhere)

This is the Databricks portability seam at the data layer. A `ModelSpec` uses filesystem paths in local development:

```python
Table(path=".causalops/data/uplift/shared_v3.parquet", ...)
```

And Unity Catalog identifiers on Databricks:

```python
Table(path="main.uplift.shared_v3", ...)
```

The planner code is identical in both cases — only the `path` values in the spec change. This is typically handled by an environment-aware helper in the spec module (see [Databricks Migration](../how-to/databricks-migration.md)).
