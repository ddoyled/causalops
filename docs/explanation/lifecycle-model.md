# The Lifecycle Model

## Why four statuses

A model version's lifecycle has four states:

```
experiment → challenger → production → retired
```

A simpler active/inactive model wouldn't serve the workflow that causal inference teams actually follow. Here's why each status exists:

**experiment**
:   The initial state when a version is registered. The model has been validated against the catalog schema and its contract is on record, but nobody has endorsed it for any purpose. It's a staging area.

**challenger**
:   The shadow-deployment slot. A challenger version runs alongside the current production version so the team can compare their results side-by-side. Consumers can query both at once:

    ```python
    client.get_results(
        family="uplift",
        status=["production", "challenger"],
        metrics=["treatment_effect"],
    )
    ```

    This returns a unioned DataFrame with a `version` column, making A/B comparison straightforward. Without a dedicated challenger status, teams would need ad-hoc version tracking outside the registry.

**production**
:   The canonical result set that downstream processes depend on. At most one version per family holds this status at any given time (see [Atomic production swaps](#atomic-production-swaps) below).

**retired**
:   A soft delete with a full audit trail. The version's registration and entire status history remain in the store — only its active status changes. Retired versions still appear in `list_versions()` and `history()`, so you can always reconstruct what happened.

## Atomic production swaps

When you promote a version to production, the store atomically retires the current production version in the same write. Both status events share the same `effective_from` timestamp:

```
v1.0.0: PRODUCTION → RETIRED  (effective_from = T)
v1.1.0: CHALLENGER → PRODUCTION (effective_from = T)
```

This atomicity prevents a window where two versions simultaneously claim production status. A downstream consumer querying `status="production"` always gets exactly one version — never zero, never two.

The `JsonFileSpecStore` achieves this by writing both events to the status log before flushing to disk. Any future store backend (e.g., Delta-backed) must provide the same guarantee, as documented in the `SpecStore.promote()` contract.

## The reactivate guard

Promoting a retired version to any other status requires an explicit `--reactivate` flag:

```bash
causalops promote --family uplift --version 1.0.0 \
    --status challenger --assigned-by "$USER" --reactivate
```

Without the flag, the promotion is rejected. This friction is intentional: un-retiring a version silently changes what downstream consumers see. If a process queries `status="challenger"`, a quietly reactivated version would appear in its results without anyone explicitly deciding that should happen.

The guard makes reactivation a deliberate act that shows up clearly in the command history and audit log.

## Append-only status log

The status log is an append-only event stream, not a mutable status field. Each event records:

- What happened (the new status)
- When it happened (`effective_from`)
- Who did it (`assigned_by`)
- Why (`note`)

This design enables **point-in-time queries**. The `as_of` parameter on `current_status()`, `by_status()`, and `get_results()` lets you ask "what was in production on 2026-01-15?" by replaying the event stream up to that timestamp. A mutable status field would lose this history.

The event stream is also preserved across re-registrations. When `--force` deletes and re-creates a registration row, the status log entries for that (family, version) pair remain. The audit trail survives even when the spec contract changes.

## Status transitions

There are no hard-coded transition rules beyond two constraints:

1. Production promotion atomically retires the outgoing production version.
2. Un-retiring requires `reactivate=True`.

Any other transition is allowed — you can promote directly from `experiment` to `production`, or from `production` back to `challenger`. The system records what happened rather than enforcing a rigid state machine, because different teams have different rollout workflows.
