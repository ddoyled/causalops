# causalops (POC)

Prototype of a shared `causalops` package that lets model repos register
`ModelSpec`s to a registry and lets consumers query results by
status/version. Local dev stores the registry in a JSON file and reads
result tables as Parquet files off the filesystem — no Hive metastore, no
long-lived Spark process. PySpark is used only by the planner and
validator.

## Documentation

Full documentation: run `mkdocs serve` or see the `docs/` directory.

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
    from causalops.utils import build_local_spark_session
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
