"""Seed the local data directory with mock Parquet tables for the examples.

Registry-of-generators pattern: each `examples/<name>/` gets an entry in
`GENERATORS`; the entry is a callable that receives the example's data
directory, version, and a per-row list of `run_dates`, and writes its
tables as Parquet files.

Timelines are hardcoded — one shadow-deployment scenario per example
family, reproducible run-to-run. Each family runs its own independent
shadow window (different `channel_id`, different date range, different
cutover date). The `registered_at` / `effective_from` timestamps on the
store's registration and status events line up with the mock data's
`run_date` window, so the status log tells the same story as the parquet
rows.

Usage
-----
    # Seed one example:
    python scripts/seed_examples.py --example uplift-model

    # Seed everything registered:
    python scripts/seed_examples.py --example all

Writes into `<repo-root>/.causalops/data/<example>/<table>.parquet` by
default. The example's `model_spec.py` points at the same paths, so
`causalops register` reads them straight off the filesystem via
`spark.read.parquet` — no Hive metastore involved.

Paths are filesystem paths in local dev. When lifting to Databricks, swap
them for UC identifiers (`catalog.schema.table`) — the planner treats a
`db.table` string as a `spark.table(...)` call and falls back to
`spark.read.parquet(...)` for anything that looks like a filesystem path.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import click
import pandas as pd

from causalops import ModelSpec, __version__
from causalops.paths import default_data_dir
from causalops.store import Status, get_store

# examples/ is not an installed package, so make it importable when this
# script is run directly (`python scripts/seed_examples.py`), which puts
# only this file's directory — not the repo root — on sys.path.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.models import (  # noqa: E402
    bsts_spec,
    scm_spec,
    seed_bsts_model,
    seed_scm_model,
    seed_uplift_model,
    uplift_spec,
)

# ---------------------------------------------------------------------------
# Timelines — one hardcoded shadow-deployment scenario per example family.
#
# Each scenario runs independently:
#
#   v1.0.0                   v1.1.0
#   ├─ prod ─────────────────┤
#                     ├─ challenger (shadow) ─┤─ prod ──┤
#
#   v1_start          v2_start                v2_promoted_at
#
# Fields:
#   channel_id            written as a column on every row (None -> omit)
#   v1_start, v1_n_days   v1.0.0's run_date window (5 rids/day)
#   v2_start, v2_n_days   v1.1.0's window; overlap with v1's tail = shadow
#   v1_registered_at      v1.0.0 registered + promoted to production
#   v2_registered_at      v1.1.0 registered as challenger
#   v2_promoted_at        shadow cutover — v1.1.0 -> prod, v1.0.0 auto-retired
# ---------------------------------------------------------------------------

RIDS_PER_DAY = 5

VERSIONS: tuple[str, ...] = ("1.0.0", "1.1.0")

SCENARIOS: dict[str, dict[str, Any]] = {
    "uplift-model": {
        "channel_id": None,
        "v1_start": "2026-01-01",
        "v1_n_days": 30,
        "v2_start": "2026-01-21",
        "v2_n_days": 20,
        "v1_registered_at": datetime(2026, 1, 1, tzinfo=UTC),
        "v2_registered_at": datetime(2026, 1, 21, tzinfo=UTC),
        "v2_promoted_at": datetime(2026, 1, 31, tzinfo=UTC),
    },
    "scm-model": {
        "channel_id": "d2c",
        "v1_start": "2026-02-01",
        "v1_n_days": 30,  # d0..d29
        "v2_start": "2026-02-21",
        "v2_n_days": 20,  # d20..d39, 10-day shadow
        "v1_registered_at": datetime(2026, 2, 1, tzinfo=UTC),
        "v2_registered_at": datetime(2026, 2, 21, tzinfo=UTC),
        "v2_promoted_at": datetime(2026, 3, 3, tzinfo=UTC),
    },
    "bsts-model": {
        "channel_id": "instacart",
        "v1_start": "2026-01-15",
        "v1_n_days": 30,  # d0..d29
        "v2_start": "2026-02-01",
        "v2_n_days": 20,  # d17..d36, 13-day shadow
        "v1_registered_at": datetime(2026, 1, 15, tzinfo=UTC),
        "v2_registered_at": datetime(2026, 2, 1, tzinfo=UTC),
        "v2_promoted_at": datetime(2026, 2, 14, tzinfo=UTC),
    },
}


def dates(start: str, n_days: int) -> list[str]:
    days = pd.date_range(start, periods=n_days, freq="D").strftime("%Y-%m-%d")
    return [d for d in days for _ in range(RIDS_PER_DAY)]


def run_dates_for(example: str, version: str) -> list[str]:
    scen = SCENARIOS[example]
    if version == "1.0.0":
        return dates(scen["v1_start"], scen["v1_n_days"])
    if version == "1.1.0":
        return dates(scen["v2_start"], scen["v2_n_days"])
    raise ValueError(f"no timeline for {example} v{version}")


# ---------------------------------------------------------------------------
# Generators — one per example.
# ---------------------------------------------------------------------------


# Add new examples here. Key MUST match the directory name under examples/.
GENERATORS: dict[str, Callable[..., None]] = {
    "uplift-model": seed_uplift_model,
    "scm-model": seed_scm_model,
    "bsts-model": seed_bsts_model,
}

# Spec factory per example, used to register + promote versions in the store.
SPEC_FACTORIES: dict[str, Callable[[Path, str], ModelSpec]] = {
    "uplift-model": uplift_spec,
    "scm-model": scm_spec,
    "bsts-model": bsts_spec,
}

REGISTERED_BY = "seed-examples"
GIT_REPO = "local/seed-examples"
GIT_SHA = "0" * 40


def bootstrap_registry(data_dir: Path, example: str) -> None:
    """Register v1.0.0/v1.1.0 for `example` and run its shadow-cutover.

    Status events are stamped with dates from this example's scenario so
    `current_status(..., as_of=...)` reflects the shadow window:

      - before v2_registered_at        : v1.0.0 = production
      - v2_registered_at .. v2_promoted-1 : v1.0.0 = production, v1.1.0 = challenger
      - from v2_promoted_at on         : v1.1.0 = production, v1.0.0 = retired
    """
    scen = SCENARIOS[example]
    spec_factory = SPEC_FACTORIES[example]
    store = get_store()

    v1_spec = spec_factory(data_dir, VERSIONS[0])
    if store.exists(v1_spec.family, v1_spec.version):
        return

    store.put(
        v1_spec,
        git_repo=GIT_REPO,
        git_tag=f"v{v1_spec.version}",
        git_sha=GIT_SHA,
        registered_by=REGISTERED_BY,
        sdk_version=__version__,
        registered_at=scen["v1_registered_at"],
    )
    store.promote(
        v1_spec.family,
        v1_spec.version,
        Status.PRODUCTION,
        assigned_by=REGISTERED_BY,
        note="initial production release",
        effective_from=scen["v1_registered_at"],
    )

    v2_spec = spec_factory(data_dir, VERSIONS[1])
    store.put(
        v2_spec,
        git_repo=GIT_REPO,
        git_tag=f"v{v2_spec.version}",
        git_sha=GIT_SHA,
        registered_by=REGISTERED_BY,
        sdk_version=__version__,
        registered_at=scen["v2_registered_at"],
    )
    store.promote(
        v2_spec.family,
        v2_spec.version,
        Status.CHALLENGER,
        assigned_by=REGISTERED_BY,
        note="challenger evaluated against production",
        effective_from=scen["v2_registered_at"],
    )

    # Shadow cutover: promoting the challenger to production atomically
    # retires v1.0.0 in the same status-log write.
    store.promote(
        v2_spec.family,
        v2_spec.version,
        Status.PRODUCTION,
        assigned_by=REGISTERED_BY,
        note="shadow cutover: challenger promoted to production",
        effective_from=scen["v2_promoted_at"],
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command()
@click.option(
    "--example",
    default="all",
    show_default=True,
    help="Example name (matches examples/<name>/) or 'all'.",
)
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    default=default_data_dir(),
    envvar="CAUSALOPS_DATA_DIR",
    show_default=True,
)
def main(example: str, data_dir: Path) -> None:
    """Seed the local data dir with example mock Parquet tables and register their specs.

    Seeds every version in `VERSIONS` against the example's hardcoded
    shadow-deployment scenario, then registers both versions and runs
    the shadow cutover (see `bootstrap_registry`).
    """
    if example != "all" and example not in GENERATORS:
        known = ", ".join(sorted(GENERATORS)) or "(none registered)"
        raise click.ClickException(f"unknown example {example!r}. Known: {known}")

    to_run = list(GENERATORS.items()) if example == "all" else [(example, GENERATORS[example])]
    for name, generator in to_run:
        scen = SCENARIOS[name]
        for version in VERSIONS:
            click.echo(f"seeding {name} v{version} ...")
            generator(
                data_dir,
                version,
                run_dates_for(name, version),
                channel_id=scen["channel_id"],
            )
        click.echo(f"registering {name} ...")
        bootstrap_registry(data_dir, name)


if __name__ == "__main__":
    main()
