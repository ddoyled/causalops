"""keystone.py — cross-family demo.

Shows how a consumer of `causalops` can:

1. Discover the latest registered version of two model families
   (`scm` and `bsts`).
2. Query both for a set of overlapping metrics.
3. Combine the results into one Spark DataFrame, tagging each row
   with the model family and version it came from.

The two families store different physical columns for the same
conceptual metric — `treatment_target_incremental` in scm vs.
`observed_target_incremental` in bsts. Their `ModelSpec`s alias
each physical column to a shared canonical name, so the consumer
queries the canonical name and never sees the difference.

Run it from the repo root:

    python examples/keystone.py

The script seeds mock parquet data and registers the specs on
first run; subsequent runs skip both.
"""

from __future__ import annotations

from examples.models.bsts import _bsts_spec, seed_bsts_model
from examples.models.scm import _scm_spec, seed_scm_model
from pyspark.sql import functions as F

from causalops import RegistryClient, __version__
from causalops.paths import default_data_dir
from causalops.spark_session import build_local_spark_session
from causalops.store import SpecStore, get_store

OVERLAPPING_METRICS = [
    "target_total",
    "target_incremental",
    "target_incremental_pct",
    "target_ci_hi",
    "target_ci_lo",
]


def _semver_key(v: str) -> tuple[int, ...]:
    return tuple(int(p) for p in v.split("."))


def latest_version(client: RegistryClient, family: str) -> str:
    versions = client.list_versions(family)
    if not versions:
        raise LookupError(f"no registered versions for family {family!r}")
    return max(versions, key=_semver_key)


def combined_latest(
    client: RegistryClient,
    families: list[str],
    metrics: list[str],
):
    """Union the latest version of each family, tagging rows with family + version."""
    parts = []
    for family in families:
        version = latest_version(client, family)
        df = (
            client.get_results(family=family, version=version, metrics=metrics)
            .withColumn("family", F.lit(family))
            .withColumn("version", F.lit(version))
        )
        parts.append(df)

    combined = parts[0]
    for df in parts[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    return combined


def _bootstrap(store: SpecStore) -> None:
    """Seed parquet + register two versions of each family, if missing.

    Registering 1.0.0 and 1.1.0 makes `latest_version` a meaningful pick
    rather than the only pick. Both versions point at the same parquet
    file, which is fine — the demo is about the discovery API, not real
    version drift.
    """
    data_dir = default_data_dir()
    if not (data_dir / "scm" / "results_v1.parquet").exists():
        seed_scm_model(data_dir)
    if not (data_dir / "bsts" / "results_v1.parquet").exists():
        seed_bsts_model(data_dir)

    specs = [
        _scm_spec(data_dir, "1.0.0"),
        _scm_spec(data_dir, "1.1.0"),
        _bsts_spec(data_dir, "1.0.0"),
        _bsts_spec(data_dir, "1.1.0"),
    ]
    for spec in specs:
        if store.exists(spec.family, spec.version):
            continue
        store.put(
            spec,
            git_repo="local/keystone-demo",
            git_tag=f"v{spec.version}",
            git_sha="0" * 40,
            registered_by="keystone-demo",
            sdk_version=__version__,
        )


def main() -> None:
    spark = build_local_spark_session()
    store = get_store()
    _bootstrap(store)

    client = RegistryClient(store=store, spark=spark)

    for family in ("scm", "bsts"):
        print(
            f"{family}: registered versions = {client.list_versions(family)}, "
            f"latest = {latest_version(client, family)}"
        )

    combined = combined_latest(
        client,
        families=["scm", "bsts"],
        metrics=OVERLAPPING_METRICS,
    )

    print()
    print("Combined overlapping metrics from latest scm + bsts:")
    combined.show(10, truncate=False)
    print(f"total rows: {combined.count()}")


if __name__ == "__main__":
    main()
