# Register and Validate

In this tutorial you will validate a model spec against its physical tables
and then register it in the causalops registry.

**Audience:** Model producer.

**Prerequisites:** [Define a ModelSpec](define-a-model-spec.md) complete,
with a `model_spec.py` defining an uplift spec.

---

## Step 1: Seed mock data

Before validating, the Parquet files referenced by the spec need to exist.
The seed script generates mock data for the example models:

```bash
python scripts/seed_examples.py --example uplift-model
```

This creates Parquet files under `.causalops/data/uplift/`.

## Step 2: Validate the spec

The `validate` command checks that every table and column declared in the spec
actually exists with the expected dtype — without registering anything:

```bash
causalops validate --spec-path model_spec.py
```

On success:

```
ok: uplift@3.1.0
```

### Understanding validation outcomes

Validation distinguishes two failure modes:

| Condition | Severity | Exit code |
|-----------|----------|-----------|
| Table does not exist yet | Warning | 0 |
| Column missing in table | Error | Non-zero |
| Column dtype mismatch | Error | Non-zero |

!!! note
    A missing table produces a **warning**, not an error. This is intentional —
    on first registration the pipeline may not have run yet, so the table does
    not exist. Missing columns and dtype mismatches are hard errors because
    they indicate real drift between the spec and the data.

If you have a dtype mismatch, the output looks like:

```
Errors:
  .causalops/data/uplift/shared_v3.1.0.parquet.ate: spec says double, table has string
```

## Step 3: Register the spec

The `register` command validates the spec and then persists a registration
record with its initial `experiment` status:

```bash
causalops register \
    --spec-path model_spec.py \
    --git-repo local/uplift-model \
    --git-tag v3.1.0 \
    --git-sha $(git rev-parse HEAD) \
    --registered-by "$USER"
```

On success:

```
Registered: uplift@3.1.0 (sha=a1b2c3d4)
```

### Git tag must match spec version

The `--git-tag` value must be `v` followed by the spec's `version` field. If
the spec says `version="3.1.0"`, the tag must be `v3.1.0`. A mismatch fails
registration:

```
Error: Git tag 'v3.0.0' does not match spec version (expected 'v3.1.0')
```

### Environment variable fallbacks

In CI, you can skip the flags entirely — the CLI reads GitHub Actions
environment variables by default:

| Flag | Environment variable |
|------|---------------------|
| `--git-repo` | `GITHUB_REPOSITORY` |
| `--git-tag` | `GITHUB_REF_NAME` |
| `--git-sha` | `GITHUB_SHA` |
| `--registered-by` | `GITHUB_ACTOR` |

So in a GitHub Actions workflow, `causalops register --spec-path model_spec.py`
is sufficient — all four values come from the environment.

## Step 4: Re-registration with --force

Attempting to register the same (family, version) pair twice fails:

```
Error: uplift@3.1.0 already registered. Use --force to overwrite.
```

Use `--force` to overwrite:

```bash
causalops register \
    --spec-path model_spec.py \
    --git-repo local/uplift-model \
    --git-tag v3.1.0 \
    --git-sha $(git rev-parse HEAD) \
    --registered-by "$USER" \
    --force
```

!!! warning
    `--force` deletes the existing registration row but preserves the status
    history. Use it only when correcting a spec error — the audit trail of
    status events is not lost.

## Step 5: Inspect the registry

The default JSON-file backend stores everything in `.causalops/registry.json`.
Open it to see the registration and its initial status event:

```bash
python -m json.tool .causalops/registry.json
```

You will see two arrays:

- **`registrations`** — one entry per (family, version) with the serialized
  spec, git metadata, and registration timestamp.
- **`status_log`** — an append-only list of status events. The initial
  registration creates an `experiment` event automatically.

```json
{
  "registrations": [
    {
      "family": "uplift",
      "version": "3.1.0",
      "spec_json": "...",
      "git_repo": "local/uplift-model",
      "git_tag": "v3.1.0",
      "git_sha": "a1b2c3d4...",
      "sdk_version": "0.1.0",
      "registered_by": "alice",
      "registered_at": "2026-09-06T12:00:00+00:00"
    }
  ],
  "status_log": [
    {
      "event_id": "...",
      "family": "uplift",
      "version": "3.1.0",
      "status": "experiment",
      "effective_from": "2026-09-06T12:00:00+00:00",
      "assigned_by": "alice",
      "note": ""
    }
  ]
}
```

---

**Next:** [Promote through the Lifecycle](promote-lifecycle.md) — move the
registered version from experiment through challenger to production.
