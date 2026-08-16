"""causalops CLI: register and promote."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import click

from causalops import ModelSpec, __version__
from causalops.store import get_store
from causalops.store.json_file import JsonFileSpecStore
from causalops.validation import validate_against_uc


def _load_spec(spec_path: Path) -> ModelSpec:
    module_spec = importlib.util.spec_from_file_location("model_spec", spec_path)
    if module_spec is None or module_spec.loader is None:
        raise click.ClickException(f"cannot load spec module from {spec_path}")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules["model_spec"] = module
    module_spec.loader.exec_module(module)
    if not hasattr(module, "spec"):
        raise click.ClickException(f"{spec_path} must define a top-level `spec` variable")
    if not isinstance(module.spec, ModelSpec):
        raise click.ClickException(
            f"`spec` in {spec_path} must be a ModelSpec, got {type(module.spec).__name__}"
        )
    return module.spec


def _build_spark():
    """Lazy import — only `register` needs Spark (for validation)."""
    from causalops.spark_session import build_local_spark_session

    return build_local_spark_session(app_name="causalops_cli")


@click.group()
@click.version_option(__version__)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """causalops CLI.

    Tests can inject a pre-built store (and, for `register`, a Spark session)
    by passing `obj={"store": ..., "spark": ...}` to `CliRunner.invoke`.
    """
    ctx.ensure_object(dict)
    if "store" not in ctx.obj:
        ctx.obj["store"] = get_store()


@cli.command()
@click.option(
    "--spec-path",
    type=click.Path(exists=True, path_type=Path),
    default=Path("model_spec.py"),
    show_default=True,
)
@click.option("--git-repo", required=True, envvar="GITHUB_REPOSITORY")
@click.option("--git-tag", required=True, envvar="GITHUB_REF_NAME")
@click.option("--git-sha", required=True, envvar="GITHUB_SHA")
@click.option("--registered-by", required=True, envvar="GITHUB_ACTOR")
@click.option(
    "--force", is_flag=True, help="Overwrite an existing registration for this (family, version)."
)
@click.pass_context
def register(ctx, spec_path, git_repo, git_tag, git_sha, registered_by, force):
    """Register a ModelSpec from a local model_spec.py."""
    store = ctx.obj["store"]

    spec = _load_spec(spec_path)

    expected_tag = f"v{spec.version}"
    if git_tag != expected_tag:
        raise click.ClickException(
            f"Git tag {git_tag!r} does not match spec version (expected {expected_tag!r})"
        )

    spark = ctx.obj.get("spark") or _build_spark()
    report = validate_against_uc(spec, spark=spark)
    if report.has_errors:
        click.echo(report.format(), err=True)
        raise click.ClickException("UC validation failed")
    if report.has_warnings:
        click.echo(report.format_warnings(), err=True)

    if store.exists(spec.family, spec.version):
        if not force:
            raise click.ClickException(
                f"{spec.family}@{spec.version} already registered. Use --force to overwrite."
            )
        click.echo(f"warning: --force overwrite of {spec.family}@{spec.version}", err=True)
        # --force is best-effort for the POC: drop the prior registration row
        # so put() can insert a fresh one. Status history is preserved.
        if isinstance(store, JsonFileSpecStore):
            store.delete(spec.family, spec.version)
        else:
            raise click.ClickException(
                f"--force is not supported for store backend {type(store).__name__!r}"
            )

    reg = store.put(
        spec,
        git_repo=git_repo,
        git_tag=git_tag,
        git_sha=git_sha,
        registered_by=registered_by,
        sdk_version=__version__,
    )
    click.echo(f"Registered: {reg.family}@{reg.version} (sha={reg.git_sha[:8]})")


@cli.command()
@click.option(
    "--spec-path",
    type=click.Path(exists=True, path_type=Path),
    default=Path("model_spec.py"),
    show_default=True,
)
@click.pass_context
def validate(ctx, spec_path):
    """Dry-run schema check for a ModelSpec (no registration).

    Warnings (missing tables) print to stderr and exit 0. Errors (missing
    columns, dtype mismatches) print to stderr and exit non-zero.
    """
    spec = _load_spec(spec_path)
    spark = ctx.obj.get("spark") or _build_spark()
    report = validate_against_uc(spec, spark=spark)
    if report.has_errors:
        click.echo(report.format(), err=True)
        raise click.ClickException("validation failed")
    if report.has_warnings:
        click.echo(report.format_warnings(), err=True)
    click.echo(f"ok: {spec.family}@{spec.version}")


@cli.command()
@click.option("--family", required=True)
@click.option("--version", required=True)
@click.option(
    "--status",
    type=click.Choice(["experiment", "challenger", "production", "retired"]),
    required=True,
)
@click.option("--assigned-by", required=True, envvar="USER")
@click.option("--note", default="")
@click.option(
    "--reactivate", is_flag=True, help="Required when un-retiring a version (safety guard)."
)
@click.pass_context
def promote(ctx, family, version, status, assigned_by, note, reactivate):
    """Append a status event for (family, version)."""
    from causalops.store.base import Status

    store = ctx.obj["store"]
    try:
        store.promote(
            family,
            version,
            Status(status),
            assigned_by=assigned_by,
            note=note,
            reactivate=reactivate,
        )
    except (ValueError, KeyError) as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"Promoted: {family}@{version} -> {status}")
