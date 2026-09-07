# Automate Registration with GitHub Actions

Register a `ModelSpec` automatically every time a semver tag is pushed.

## How it works

The `causalops register` CLI reads git metadata from environment variables that GitHub Actions sets automatically:

| CLI flag | Environment variable | Set by GHA |
|----------|---------------------|------------|
| `--git-repo` | `GITHUB_REPOSITORY` | Yes |
| `--git-tag` | `GITHUB_REF_NAME` | Yes |
| `--git-sha` | `GITHUB_SHA` | Yes |
| `--registered-by` | `GITHUB_ACTOR` | Yes |

This means the `register` command needs no explicit flags in a GHA context.

## Workflow file

Create `.github/workflows/register.yml`:

```yaml
name: Register model spec
on:
  push:
    tags: ['v*.*.*']

jobs:
  register:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Install SDK
        run: pip install causalops
      - name: Type-check the spec
        run: |
          pip install mypy
          mypy model_spec.py --strict
      - name: Register
        env:
          CAUSALOPS_STORE_CONFIG: ${{ vars.CAUSALOPS_STORE_CONFIG }}
        run: causalops register --spec-path model_spec.py
```

## Step-by-step breakdown

### 1. Trigger on semver tags

```yaml
on:
  push:
    tags: ['v*.*.*']
```

The workflow fires only when you push a tag matching the semver pattern (e.g., `v3.1.0`). The CLI validates that the tag matches the `version` field in the spec — `v3.1.0` must correspond to `version="3.1.0"` in your `ModelSpec`.

### 2. Type-check the spec

```yaml
- name: Type-check the spec
  run: |
    pip install mypy
    mypy model_spec.py --strict
```

Running `mypy --strict` catches type errors in the spec definition before registration. This is optional but recommended — it verifies that `Metric`, `Table`, and `ModelSpec` types are used correctly.

### 3. Configure the store backend

```yaml
env:
  CAUSALOPS_STORE_CONFIG: ${{ vars.CAUSALOPS_STORE_CONFIG }}
```

Set `CAUSALOPS_STORE_CONFIG` as a repository variable in GitHub Settings. For a Databricks-backed registry:

```json
{"backend": "delta", "database": "main.registry"}
```

For the default JSON file store (local dev or testing), omit the variable entirely.

### 4. Permissions for Databricks auth

```yaml
permissions:
  id-token: write
  contents: read
```

The `id-token: write` permission is needed if your registration step authenticates to Databricks using OIDC (workload identity federation). If you use a service principal token instead, you only need `contents: read`.

!!! note
    The register step also runs `validate_against_uc`, which reads the declared tables to check column names and dtypes. Make sure the runner can reach the data catalog (Databricks workspace or local Parquet files).

## Tagging workflow

```bash
# In your model repo, after updating model_spec.py:
git tag v3.2.0
git push origin v3.2.0
```

The workflow picks up the tag, validates the spec, and registers it to the configured store.
