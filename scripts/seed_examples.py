"""Seed the local data directory with mock Parquet tables for the examples.

Registry-of-generators pattern: each `examples/<name>/` gets an entry in
`GENERATORS`; the entry is a callable that receives the example's data
directory and writes its tables as Parquet files.

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
from pathlib import Path

import click

from causalops.paths import default_data_dir

# examples/ is not an installed package, so make it importable when this
# script is run directly (`python scripts/seed_examples.py`), which puts
# only this file's directory — not the repo root — on sys.path.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from examples.models import seed_bsts_model, seed_scm_model, seed_uplift_model  # noqa: E402

# ---------------------------------------------------------------------------
# Generators — one per example.
# ---------------------------------------------------------------------------


# Add new examples here. Key MUST match the directory name under examples/.
GENERATORS: dict[str, Callable[[Path], None]] = {
    "uplift-model": seed_uplift_model,
    "scm-model": seed_scm_model,
    "bsts-model": seed_bsts_model,
}


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
    """Seed the local data dir with example mock Parquet tables."""
    if example != "all" and example not in GENERATORS:
        known = ", ".join(sorted(GENERATORS)) or "(none registered)"
        raise click.ClickException(f"unknown example {example!r}. Known: {known}")

    to_run = list(GENERATORS.items()) if example == "all" else [(example, GENERATORS[example])]
    for name, generator in to_run:
        click.echo(f"seeding {name} ...")
        generator(data_dir)


if __name__ == "__main__":
    main()
