"""Default filesystem locations for local Spark state.

The CLI and seed script both need a stable default for the warehouse and
metastore. Anchoring them to the repo root (rather than `~/.causalops`) keeps
each checkout self-contained: `git clean -fdx` or deleting the repo drops the
local state with it, and multiple checkouts don't share a warehouse.
"""

from __future__ import annotations

from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    """Walk up from `start` (or cwd) for a `pyproject.toml`; fall back to cwd."""
    start = (start or Path.cwd()).resolve()
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return Path.cwd().resolve()


def default_warehouse_dir() -> Path:
    return find_repo_root() / ".causalops" / "warehouse"


def default_metastore_dir() -> Path:
    return find_repo_root() / ".causalops" / "metastore_db"
