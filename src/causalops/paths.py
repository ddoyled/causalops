"""Default filesystem locations for local dev state.

The CLI and seed script both need stable defaults for the registry file and
the mock Parquet data. Anchoring them under `<repo>/.causalops/` (rather
than `~/.causalops/`) keeps each checkout self-contained: `git clean -fdx`
or deleting the repo drops the local state with it, and multiple checkouts
don't share a registry.
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


def default_data_dir() -> Path:
    """Where the seed script writes mock Parquet tables (and where specs point)."""
    return find_repo_root() / ".causalops" / "data"


def default_registry_json_path() -> Path:
    """Where the JSON-file SpecStore keeps its registrations + status log."""
    return find_repo_root() / ".causalops" / "registry.json"
