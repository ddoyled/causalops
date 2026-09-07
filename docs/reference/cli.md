# CLI Reference

The `causalops` command-line interface provides three commands for registering
specs, validating schemas, and promoting versions through the lifecycle.

```
causalops [OPTIONS] COMMAND [ARGS]...
```

**Options:**

| Option | Description |
|--------|-------------|
| `--version` | Show the version and exit. |
| `--help` | Show this message and exit. |

---

## `register`

Register a ModelSpec from a local `model_spec.py`.

```
causalops register [OPTIONS]
```

**Options:**

| Option | Default | Env var | Description |
|--------|---------|---------|-------------|
| `--spec-path PATH` | `model_spec.py` | | Path to a Python file defining a `spec` variable of type `ModelSpec`. |
| `--git-repo TEXT` | *(required)* | `GITHUB_REPOSITORY` | Repository identifier (e.g. `org/repo`). |
| `--git-tag TEXT` | *(required)* | `GITHUB_REF_NAME` | Git tag — must match `v{spec.version}`. |
| `--git-sha TEXT` | *(required)* | `GITHUB_SHA` | Full commit SHA. |
| `--registered-by TEXT` | *(required)* | `GITHUB_ACTOR` | Who is registering this spec. |
| `--force` | `false` | | Overwrite an existing registration for this (family, version). |

The command loads the spec from `--spec-path`, validates that `--git-tag`
matches `v{spec.version}`, runs schema validation against the live tables,
and persists the registration with an initial `experiment` status.

---

## `validate`

Dry-run schema check for a ModelSpec (no registration).

```
causalops validate [OPTIONS]
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--spec-path PATH` | `model_spec.py` | Path to a Python file defining a `spec` variable. |

Warnings (missing tables) print to stderr and exit 0. Errors (missing
columns, dtype mismatches) print to stderr and exit non-zero.

---

## `promote`

Append a status event for (family, version).

```
causalops promote [OPTIONS]
```

**Options:**

| Option | Default | Env var | Description |
|--------|---------|---------|-------------|
| `--family TEXT` | *(required)* | | Model family name. |
| `--version TEXT` | *(required)* | | Semver version string. |
| `--status [experiment\|challenger\|production\|retired]` | *(required)* | | Target status. |
| `--assigned-by TEXT` | *(required)* | `USER` | Who is performing the promotion. |
| `--note TEXT` | `""` | | Optional note for the status event. |
| `--reactivate` | `false` | | Required when un-retiring a version (safety guard). |

Promoting to `production` atomically retires the current production version.
Un-retiring a `retired` version requires the `--reactivate` flag.
