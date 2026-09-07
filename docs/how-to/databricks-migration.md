# Migrate to Databricks

The causalops SDK is designed for lift-and-shift to Databricks. The planner, validator, and `RegistryClient` code run unchanged — only two configuration seams move.

## What stays the same

Everything a consumer touches works identically on Databricks:

- `RegistryClient.get_results()` — same API, same DataFrame output
- `plan_for_spec()` / `plan_for_specs()` — same join and union logic
- `validate_against_uc()` — same schema checks
- `causalops register` / `causalops promote` — same CLI commands

## What changes

### 1. Registry backend

Swap the JSON file store for a Delta-backed store by setting the `CAUSALOPS_STORE_CONFIG` environment variable:

=== "Local dev (default)"

    ```bash
    # No config needed — defaults to .causalops/registry.json
    ```

=== "Databricks"

    ```bash
    export CAUSALOPS_STORE_CONFIG='{"backend": "delta", "database": "main.registry"}'
    ```

The Delta-backed store implements the same [`SpecStore`][causalops.store.base.SpecStore] ABC, so all consumer and CLI code works without changes. See [Custom Store Backend](custom-store-backend.md) for implementation details.

### 2. Result table paths

`Table.path` values in your `model_spec.py` switch from filesystem paths to Unity Catalog identifiers:

=== "Local dev"

    ```python
    Table(
        name="shared",
        path=".causalops/data/uplift/shared_v3.parquet",
        key="experiment_id",
        metrics=(...),
    )
    ```

=== "Databricks"

    ```python
    Table(
        name="shared",
        path="main.uplift.shared_v3",
        key="experiment_id",
        metrics=(...),
    )
    ```

The `read_table` helper in `causalops.utils` handles this transparently: if the path contains `/` or `://`, it reads via `spark.read.parquet()`; otherwise it reads via `spark.table()`. No planner code changes.

!!! tip "Environment-aware paths"
    Keep `Table.path` values in a small helper inside your spec module that switches on environment:

    ```python
    import os
    from causalops.paths import default_data_dir

    def table_path(table_name: str, version: str) -> str:
        if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            return f"main.uplift.{table_name}_v{version}"
        return str(default_data_dir() / "uplift" / f"{table_name}_v{version}.parquet")
    ```

    This keeps the `ModelSpec` definition identical between environments — only the helper's return value changes.

### 3. Spark session

On Databricks, use the cluster-provided session instead of `build_local_spark_session()`:

=== "Local dev"

    ```python
    from causalops.utils import build_local_spark_session

    spark = build_local_spark_session()
    client = RegistryClient(store=get_store(), spark=spark)
    ```

=== "Databricks notebook"

    ```python
    # `spark` is already available in the notebook context
    client = RegistryClient(store=get_store(), spark=spark)
    ```

The `build_local_spark_session()` factory is only used for local dev — it creates a minimal Spark session with no Hive metastore. On a Databricks cluster, the pre-configured session already has access to Unity Catalog.

## Migration checklist

- [ ] Implement or deploy a Delta-backed `SpecStore` (see [Custom Store Backend](custom-store-backend.md))
- [ ] Set `CAUSALOPS_STORE_CONFIG` in your Databricks workspace environment
- [ ] Update `Table.path` values in `model_spec.py` to use UC identifiers (or use an env-aware helper)
- [ ] Remove `build_local_spark_session()` calls — use the cluster session
- [ ] Run `causalops validate --spec-path model_spec.py` to verify the spec resolves against live UC tables
- [ ] Update CI workflow to authenticate to Databricks and set `CAUSALOPS_STORE_CONFIG`
