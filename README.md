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
