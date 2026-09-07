# causalops (POC)

Prototype of a shared `causalops` package that lets model repos register
`ModelSpec`s to a registry and lets consumers query results by
status/version. Local dev stores the registry in a JSON file and reads
result tables as Parquet files off the filesystem — no Hive metastore, no
long-lived Spark process. PySpark is used only by the planner and
validator.

## Documentation

Full documentation: [ddoyled.github.io/causalops](https://ddoyled.github.io/causalops/)

## Setup

    uv sync --dev
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # or your Java 17 path

## Test

    uv run pytest -x -q

## Examples

Seed the local warehouse and registry, then run either example:

    uv run python scripts/seed_examples.py

[**Keystone**](examples/keystone.py) — build a production-of-record table across all families:

    uv run python examples/keystone.py

[**Champion / Challenger**](examples/champion_challenger.py) — compare two versions side-by-side:

    uv run python examples/champion_challenger.py

Local state lives under `<repo>/.causalops/` and is wiped by `git clean -fdx`.
