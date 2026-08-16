# causalops (POC)

Prototype of a shared `causalops` package that lets model repos register
`ModelSpec`s to a registry and lets consumers query results by
status/version. Local dev stores the registry in a JSON file and reads
result tables as Parquet files off the filesystem — no Hive metastore, no
long-lived Spark process. Pyspark is used only by the planner and
validator (see [Migrating to Databricks](#migrating-to-databricks)).

## Setup

    python -m venv .venv && source .venv/bin/activate
    pip install -e '.[dev]'
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # or your Java 17 path

## Test

    pytest -x -q

## Design

See `docs/superpowers/plans/2026-08-14-model-registry-poc.md`.

## Walkthrough

Seed the local Parquet tables the example spec points at:

    python scripts/seed_examples.py --example uplift-model

Register and promote:

    cd examples/uplift-model
    causalops register --spec-path model_spec.py \
        --git-repo local/uplift-model --git-tag v3.1.0 \
        --git-sha $(git rev-parse HEAD) --registered-by "$USER"
    causalops promote --family uplift --version 3.1.0 \
        --status production --assigned-by "$USER"

Query from a notebook / Python REPL:

    from causalops import RegistryClient
    from causalops.spark_session import build_local_spark_session
    from causalops.store import get_store

    spark = build_local_spark_session()
    client = RegistryClient(store=get_store(), spark=spark)
    client.get_results(
        family="uplift", status="production",
        metrics=["treatment_effect", "cate_variance"],
    ).show()

Local state lives under `<repo>/.causalops/`:

- `registry.json` — the registry (registrations + status log).
- `data/<example>/<table>.parquet` — mock result tables.

Both are wiped by `git clean -fdx`.

## Migrating to Databricks

The POC is deliberately kept lift-and-shift compatible with Databricks.
Everything a consumer touches (`RegistryClient`, the planner, the
validator) stays identical — only two config points move.

**1. Registry backend.** The default `json_file` store keeps the registry
in one JSON file. On Databricks, swap it for a Delta-backed store pointed
at a Unity Catalog schema. Set the config via env var:

    export CAUSALOPS_STORE_CONFIG='{"backend":"delta","database":"main.registry"}'

That backend isn't in the repo yet — writing it is a small addition
alongside `store/json_file.py` that implements the same `SpecStore`
contract using `DeltaTable` + `spark.sql`. (See git history for
`store/spark_hive.py`, the previous Hive-metastore version — the
UC-targeted store is the same shape with a UC 3-part identifier as
`database`.)

**2. Result table paths.** `Table.path` values in `model_spec.py` are
filesystem paths in local dev (`.causalops/data/uplift/shared_v3.parquet`)
and Unity Catalog identifiers on Databricks (`main.uplift.shared_v3`). The
planner's `read_result_table` helper (`src/causalops/data_source.py`)
inspects the string: if it contains a `/` or URL scheme, it's read via
`spark.read.parquet(...)`; otherwise it's read via `spark.table(...)`. So
the same planner code runs unchanged; you only edit the spec's `path`
values.

Typical pattern: keep `path` values in a small env-aware helper inside the
spec module (as `examples/uplift-model/model_spec.py` does with
`default_data_dir()`), and swap that helper's return value when running on
Databricks.

**3. Spark session.** `build_local_spark_session` is the local factory
(minimal, no Hive). On Databricks, use the cluster-provided session and
skip that factory entirely — pass Databricks' `spark` into
`RegistryClient(store=..., spark=spark)`.
