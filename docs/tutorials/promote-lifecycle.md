# Promote through the Lifecycle

In this tutorial you will move a registered model version through the
causalops lifecycle: experiment, challenger, production, and retired.

**Audience:** Platform team.

**Prerequisites:** [Register & Validate](register-and-validate.md) complete —
you have `uplift@3.1.0` registered with status `experiment`.

---

## The four statuses

Every registered version starts as `experiment` and progresses through a
fixed set of statuses:

```
experiment → challenger → production → retired
```

| Status | Meaning |
|--------|---------|
| `experiment` | Initial state after registration. Under development. |
| `challenger` | Shadow-deployed alongside the current production model. |
| `production` | The active production version. At most one per family. |
| `retired` | Replaced by a newer production version. |

!!! note
    There is no restriction on which transitions are allowed (you can go from
    `experiment` straight to `production`), but the `retired` → anything
    transition requires the `--reactivate` flag as a safety guard.

## Step 1: Promote to challenger

```bash
causalops promote \
    --family uplift \
    --version 3.1.0 \
    --status challenger \
    --assigned-by "$USER"
```

Output:

```
Promoted: uplift@3.1.0 -> challenger
```

You can optionally attach a note:

```bash
causalops promote \
    --family uplift \
    --version 3.1.0 \
    --status challenger \
    --assigned-by "$USER" \
    --note "shadow deployment started"
```

## Step 2: Promote to production

When the challenger has been validated, promote it to production:

```bash
causalops promote \
    --family uplift \
    --version 3.1.0 \
    --status production \
    --assigned-by "$USER" \
    --note "graduated from shadow"
```

### Atomic production swap

If another version of the same family is already in production, it is
**automatically retired** in the same operation. Both status events share the
same `effective_from` timestamp, ensuring there is never a moment where two
versions claim production simultaneously.

For example, if `uplift@3.0.0` was in production, promoting `uplift@3.1.0`
to production atomically:

1. Writes a `production` event for `uplift@3.1.0`.
2. Writes a `retired` event for `uplift@3.0.0`.

Both events have the same timestamp.

## Step 3: The reactivate guard

Once a version is retired, promoting it to any other status requires the
`--reactivate` flag:

```bash
# This fails:
causalops promote --family uplift --version 3.0.0 --status challenger --assigned-by "$USER"
# Error: uplift@3.0.0 is retired; pass --reactivate to un-retire

# This succeeds:
causalops promote --family uplift --version 3.0.0 --status challenger \
    --assigned-by "$USER" --reactivate
```

!!! warning
    Reactivating a retired version can silently change what downstream
    consumers see (e.g. if they query by status). The flag exists to make
    this a deliberate decision, not an accident.

## Step 4: Query history from Python

You can inspect the full status timeline programmatically:

```python
from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

spark = build_local_spark_session()
client = RegistryClient(store=get_store(), spark=spark)

for event in client.history("uplift", "3.1.0"):
    print(f"{event.effective_from}  {event.status.value:12s}  by {event.assigned_by}")
```

Output:

```
2026-09-06 12:00:00+00:00  experiment    by alice
2026-09-06 12:05:00+00:00  challenger    by alice
2026-09-06 12:10:00+00:00  production    by alice
```

## Step 5: Query production windows

[`production_windows()`][causalops.client.RegistryClient.production_windows]
returns each version's production stints as `(version, start_date, end_date)`
tuples:

```python
for version, start, end in client.production_windows("uplift"):
    print(f"v{version}  {start} .. {end or '(open)'}")
```

Output:

```
v3.0.0  2026-08-01 .. 2026-09-06
v3.1.0  2026-09-06 .. (open)
```

A production window opens on the `PRODUCTION` event and closes on the
subsequent `RETIRED` event. If the version is still in production, the end
date is `None`.

---

**Next:** [Query Results](query-results.md) — use the RegistryClient to
query model results by version or status.
