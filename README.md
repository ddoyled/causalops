# Model Registry POC

Prototype of a shared `registry_sdk` package that lets model repos register
`ModelSpec`s to a registry and lets consumers query results by status/version.
Backed by Delta tables in a local Hive metastore (Spark 3.5.x / DBR 16.4).

## Setup

    python -m venv .venv && source .venv/bin/activate
    pip install -e '.[dev]'
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # or your Java 17 path

## Test

    pytest -x -q

## Design

See `docs/superpowers/plans/2026-08-14-model-registry-poc.md`.

## Walkthrough

    export REGISTRY_STORE_CONFIG='{"backend":"spark_hive","database":"registry"}'
    cd examples/uplift-model
    registry-sdk register --spec-path model_spec.py \
        --git-repo local/uplift-model --git-tag v3.1.0 \
        --git-sha $(git rev-parse HEAD) --registered-by "$USER"
    registry-sdk promote --family uplift --version 3.1.0 \
        --status production --assigned-by "$USER"

Then from a notebook / Python REPL:

    from registry_sdk import RegistryClient
    from registry_sdk.spark_session import build_local_spark_session
    from registry_sdk.store import get_store

    spark = build_local_spark_session(
        warehouse_dir="~/.registry_sdk/warehouse",
        metastore_dir="~/.registry_sdk/metastore_db",
    )
    client = RegistryClient(store=get_store(spark), spark=spark)
    client.get_results(
        family="uplift", status="production",
        metrics=["treatment_effect", "cate_variance"],
    ).show()
