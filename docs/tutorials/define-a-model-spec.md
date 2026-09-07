# Define a ModelSpec

In this tutorial you will create a model spec — the versioned contract that
declares which tables and metrics a causal inference model produces.

**Audience:** Model producer (the team that owns the model).

**Prerequisites:** `pip install causalops`

---

## What is a ModelSpec?

A [`ModelSpec`][causalops.spec.ModelSpec] is a frozen Pydantic model that
describes one version of a model family's result tables. It declares:

- The **family** name (e.g. `"uplift"`, `"scm"`, `"bsts"`).
- A **semver version** like `"3.1.0"`.
- A **measurement key** that joins rows across tables.
- One or more **tables**, each containing **metrics** that map consumer-facing
  canonical names to physical columns.

## Step 1: Create a spec file

Create a file called `model_spec.py`. This is the file that the `causalops`
CLI will load when you register or validate.

```python
from causalops import Metric, ModelSpec, Table
```

## Step 2: Define metrics

Each [`Metric`][causalops.spec.Metric] maps a canonical name to a physical
column in a result table:

```python
treatment_effect = Metric(
    name="treatment_effect",   # consumer-facing canonical name
    column="ate",              # physical column in the Parquet/UC table
    dtype="double",
    aliases=("te",),           # former names still queryable
)
ci_lower = Metric(name="ci_lower", column="ci_lo", dtype="double")
ci_upper = Metric(name="ci_upper", column="ci_hi", dtype="double")
```

The `aliases` tuple lets you rename a metric across versions without breaking
downstream queries — consumers who still request `"te"` will get the column
named `"treatment_effect"`.

## Step 3: Group metrics into tables

A [`Table`][causalops.spec.Table] groups related metrics and points at the
physical data:

```python
shared_table = Table(
    name="shared",
    path=".causalops/data/uplift/shared_v3.1.0.parquet",
    key="experiment_id",
    metrics=(treatment_effect, ci_lower, ci_upper),
)

heterogeneity_table = Table(
    name="heterogeneity",
    path=".causalops/data/uplift/het_v3.1.0.parquet",
    key="experiment_id",
    metrics=(
        Metric(
            name="cate_variance",
            column="het_score",
            dtype="double",
            aliases=("heterogeneity_score",),
        ),
    ),
)
```

!!! note
    `path` is a filesystem path in local dev and a Unity Catalog identifier
    (e.g. `main.uplift.shared_v3`) on Databricks. The planner handles both
    transparently.

## Step 4: Compose the ModelSpec

```python
spec = ModelSpec(
    family="uplift",
    version="3.1.0",
    measurement_key="experiment_id",
    tables=(shared_table, heterogeneity_table),
)
```

The complete `model_spec.py` should define a top-level variable called `spec` —
this is what the CLI loads.

??? example "Full model_spec.py"
    ```python
    from causalops import Metric, ModelSpec, Table

    spec = ModelSpec(
        family="uplift",
        version="3.1.0",
        measurement_key="experiment_id",
        tables=(
            Table(
                name="shared",
                path=".causalops/data/uplift/shared_v3.1.0.parquet",
                key="experiment_id",
                metrics=(
                    Metric(
                        name="treatment_effect",
                        column="ate",
                        dtype="double",
                        aliases=("te",),
                    ),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                    Metric(name="ci_upper", column="ci_hi", dtype="double"),
                ),
            ),
            Table(
                name="heterogeneity",
                path=".causalops/data/uplift/het_v3.1.0.parquet",
                key="experiment_id",
                metrics=(
                    Metric(
                        name="cate_variance",
                        column="het_score",
                        dtype="double",
                        aliases=("heterogeneity_score",),
                    ),
                ),
            ),
        ),
    )
    ```

## Step 5: Explore validation rules

causalops validates your spec at construction time. Here are the most common
errors you will see:

### Invalid semver

The version must be three-part `MAJOR.MINOR.PATCH`:

```python
ModelSpec(family="uplift", version="v3.1", measurement_key="eid", tables=())
# ValidationError: version 'v3.1' must be MAJOR.MINOR.PATCH (e.g. 3.1.0)
```

### Duplicate metric names

Two metrics in the same table cannot share a canonical name:

```python
Table(
    name="t",
    path="p",
    key="k",
    metrics=(
        Metric(name="x", column="a", dtype="double"),
        Metric(name="x", column="b", dtype="double"),
    ),
)
# ValidationError: duplicate metric 'x' in table 't'
```

### Alias collides with canonical name

An alias cannot match another metric's canonical name within the same table:

```python
Table(
    name="t",
    path="p",
    key="k",
    metrics=(
        Metric(name="x", column="a", dtype="double"),
        Metric(name="y", column="b", dtype="double", aliases=("x",)),
    ),
)
# ValidationError: alias 'x' collides with an existing metric name or alias in table 't'
```

### Cross-table collision

A metric lookup name must be unique across all tables in the spec:

```python
ModelSpec(
    family="f",
    version="1.0.0",
    measurement_key="k",
    tables=(
        Table(name="a", path="p1", key="k", metrics=(Metric(name="x", column="c1", dtype="double"),)),
        Table(name="b", path="p2", key="k", metrics=(Metric(name="x", column="c2", dtype="double"),)),
    ),
)
# ValidationError: cross-table collision: metric lookup 'x' appears in tables 'a' and 'b'
```

## Step 6: Resolve metrics

Use [`resolve_metric()`][causalops.spec.ModelSpec.resolve_metric] to verify
that a canonical name or alias resolves correctly:

```python
table, canonical, column = spec.resolve_metric("te")
print(table.name)   # "shared"
print(canonical)     # "treatment_effect"
print(column)        # "ate"
```

Unknown metrics raise `KeyError`:

```python
spec.resolve_metric("nonexistent")
# KeyError: "unknown metric 'nonexistent' in spec uplift@3.1.0"
```

## Step 7: JSON round-trip

Specs are serializable — useful for storage and debugging:

```python
json_str = spec.model_dump_json(indent=2)
print(json_str)

restored = ModelSpec.model_validate_json(json_str)
assert restored == spec
```

Because all models are frozen (immutable), they are also hashable and can be
used as dictionary keys.

---

**Next:** [Register & Validate](register-and-validate.md) — take this spec
through schema validation and register it in the local registry.
