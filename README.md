# causalops (POC)

Prototype of a shared `causalops` package that lets model repos register
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

    export CAUSALOPS_STORE_CONFIG='{"backend":"spark_hive","database":"registry"}'
    cd examples/uplift-model
    causalops register --spec-path model_spec.py \
        --git-repo local/uplift-model --git-tag v3.1.0 \
        --git-sha $(git rev-parse HEAD) --registered-by "$USER"
    causalops promote --family uplift --version 3.1.0 \
        --status production --assigned-by "$USER"

Then from a notebook / Python REPL:

    from causalops import RegistryClient
    from causalops.spark_session import build_local_spark_session
    from causalops.store import get_store

    from causalops.paths import default_metastore_dir, default_warehouse_dir

    spark = build_local_spark_session(
        warehouse_dir=default_warehouse_dir(),
        metastore_dir=default_metastore_dir(),
    )
    client = RegistryClient(store=get_store(spark), spark=spark)
    client.get_results(
        family="uplift", status="production",
        metrics=["treatment_effect", "cate_variance"],
    ).show()
