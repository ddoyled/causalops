# Model Registry POC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python SDK (`registry_sdk`) that lets model repos register versioned `ModelSpec`s to a shared registry (two Delta tables in a local Hive metastore), promote versions through experiment/challenger/production statuses, and lets consumers query results by status or version via a canonical/alias-resolving PySpark planner.

**Architecture:** One SDK does triple duty — authoring (Pydantic `ModelSpec`), registration (CLI writes to registry), consumption (planner reads registry + queries result tables). Two Delta-backed tables: `registrations` (immutable, one row per `(family, version)`) and `status_log` (append-only status events). Storage lives behind a `SpecStore` ABC so the Spark/Hive impl is swappable. Local dev uses PySpark 3.5.x with embedded-Derby Hive metastore and Delta Lake; matches DBR 16.4's Spark surface.

**Tech Stack:**
- Python 3.12 (DBR 16.4 baseline)
- PySpark 3.5.2 with `enableHiveSupport()` (embedded Derby metastore)
- delta-spark 3.2.x for ACID and multi-row atomic inserts
- Pydantic v2 for spec models and JSON round-trip
- Click for CLI (`registry-sdk register`, `registry-sdk promote`)
- pytest for tests, `CliRunner` for CLI tests
- Java 17 (Spark 3.5 requirement — check `java -version` before starting)

**Repo layout (target):**
```
ci_reg_poc/
├── pyproject.toml
├── README.md
├── src/registry_sdk/
│   ├── __init__.py             # exports ModelSpec, Table, Metric, RegistryClient
│   ├── spec.py                 # Pydantic models
│   ├── spark_session.py        # local Spark session factory
│   ├── validation.py           # validate_against_uc()
│   ├── planner.py              # plan_for_spec / plan_for_specs
│   ├── client.py               # RegistryClient (consumer API)
│   ├── cli.py                  # click entry points
│   └── store/
│       ├── __init__.py         # get_store() factory
│       ├── base.py             # SpecStore ABC, Registration dataclass
│       └── spark_hive.py       # SparkHiveSpecStore
├── tests/
│   ├── conftest.py             # spark fixture, tmp warehouse, store fixture
│   ├── test_spec.py
│   ├── test_spark_session.py
│   ├── test_store_init.py
│   ├── test_store_put_get.py
│   ├── test_store_status.py
│   ├── test_store_promote.py
│   ├── test_validation.py
│   ├── test_cli_register.py
│   ├── test_cli_promote.py
│   ├── test_planner.py
│   └── test_client.py
└── examples/uplift-model/
    ├── model_spec.py
    └── .github/workflows/register.yml
```

**Test strategy:** Every task is TDD — failing test first, minimal impl, green, commit. A session-scoped `spark` fixture builds one Spark session per test run (Spark startup is ~5–10s); each test that needs a fresh registry gets a function-scoped `store` fixture that creates a tmp warehouse dir and its own database. Result-table queries in planner/client tests create fake Delta tables in the tmp warehouse.

**Conventions:**
- All commits use conventional-commit prefixes (`feat:`, `test:`, `chore:`, `docs:`).
- Every task ends with running the full test suite (`pytest -x -q`) before commit.
- No `# type: ignore` without a comment explaining why.

---

## Task 1: Project scaffolding + Spark smoke test

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/pyproject.toml`
- Create: `/mnt/d/Code/ci_reg_poc/README.md`
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/__init__.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/__init__.py`
- Create: `/mnt/d/Code/ci_reg_poc/.gitignore`

- [ ] **Step 1: Verify Java 17 is available**

Run: `java -version 2>&1 | head -1`
Expected: `openjdk version "17.` (or `"11.` at minimum, but 17 is DBR-aligned)

If missing: `sudo apt-get install -y openjdk-17-jdk-headless` and set `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "registry-sdk"
version = "0.1.0"
description = "Model registry SDK for causal inference results on Databricks"
requires-python = ">=3.12"
dependencies = [
    "pydantic>=2.6,<3",
    "click>=8.1,<9",
    "pyspark==3.5.2",
    "delta-spark==3.2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "pytest-xdist>=3",
    "mypy>=1.10",
]

[project.scripts]
registry-sdk = "registry_sdk.cli:cli"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra"
filterwarnings = [
    "ignore::DeprecationWarning:pyspark.*",
    "ignore::DeprecationWarning:pkg_resources.*",
]
```

- [ ] **Step 3: Write `.gitignore`**

```gitignore
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/
*.egg-info/
build/
dist/
.venv/
venv/
# Local Spark artifacts
metastore_db/
spark-warehouse/
derby.log
```

- [ ] **Step 4: Write `README.md`**

```markdown
# Model Registry POC

Prototype of a shared `registry_sdk` package that lets model repos register
`ModelSpec`s to a registry and lets consumers query results by status/version.
Backed by Delta tables in a local Hive metastore (Spark 3.5.x / DBR 16.4).

## Setup

    python -m venv .venv && source .venv/bin/activate
    pip install -e '.[dev]'
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # or your Java 17 path

## Test

    pytest -x -q

## Design

See `docs/superpowers/plans/2026-08-14-model-registry-poc.md`.
```

- [ ] **Step 5: Create empty package files**

Write `/mnt/d/Code/ci_reg_poc/src/registry_sdk/__init__.py`:

```python
"""registry_sdk — model registry SDK."""

__version__ = "0.1.0"
```

Write `/mnt/d/Code/ci_reg_poc/tests/__init__.py` (empty file).

- [ ] **Step 6: Init git repo + create virtualenv + install**

```bash
cd /mnt/d/Code/ci_reg_poc
git init -b main
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

Expected: install completes, `registry-sdk --help` fails with `No such command` (cli module doesn't exist yet — that's fine).

- [ ] **Step 7: Spark smoke test — verify local Spark + Delta start**

Create `/mnt/d/Code/ci_reg_poc/tests/test_smoke.py`:

```python
"""Smoke test: verify PySpark + Delta start locally with Hive support."""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def test_spark_delta_hive_starts(tmp_path):
    warehouse = tmp_path / "warehouse"
    metastore = tmp_path / "metastore_db"
    builder = (
        SparkSession.builder
        .appName("smoke")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS smoke_db")
        spark.sql(
            "CREATE TABLE smoke_db.t (x INT) USING DELTA"
        )
        spark.sql("INSERT INTO smoke_db.t VALUES (1), (2)")
        rows = spark.sql("SELECT sum(x) AS s FROM smoke_db.t").collect()
        assert rows[0]["s"] == 3
    finally:
        spark.stop()
```

- [ ] **Step 8: Run smoke test**

Run: `pytest tests/test_smoke.py -v -x`
Expected: PASS (first run downloads delta jars via Ivy — up to 60s). If Ivy fails behind a proxy, set `SPARK_LOCAL_IP=127.0.0.1` and retry.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "chore: scaffold registry_sdk package with pyspark+delta smoke test"
```

---

## Task 2: Pydantic spec models with JSON round-trip

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/spec.py`
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/__init__.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_spec.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_spec.py`:

```python
"""Tests for ModelSpec / Table / Metric Pydantic models."""
import json

import pytest
from pydantic import ValidationError

from registry_sdk import Metric, ModelSpec, Table


def _sample_spec() -> ModelSpec:
    return ModelSpec(
        family="uplift",
        version="3.1.0",
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path="analytics.uplift.shared_v3",
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column="ate",
                           dtype="double", aliases=["te"]),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                ],
            ),
            Table(
                name="heterogeneity",
                path="analytics.uplift.het_v3",
                key="experiment_id",
                metrics=[
                    Metric(name="cate_variance", column="het_score",
                           dtype="double", aliases=["heterogeneity_score"]),
                ],
            ),
        ],
    )


def test_spec_constructs_and_exposes_fields():
    spec = _sample_spec()
    assert spec.family == "uplift"
    assert spec.version == "3.1.0"
    assert len(spec.tables) == 2
    assert spec.tables[0].metrics[0].aliases == ["te"]


def test_spec_round_trips_through_json():
    spec = _sample_spec()
    payload = spec.model_dump_json()
    restored = ModelSpec.model_validate_json(payload)
    assert restored == spec
    # sanity: JSON is valid + stable-ish
    assert json.loads(payload)["family"] == "uplift"


def test_version_must_be_semver_like():
    with pytest.raises(ValidationError):
        ModelSpec(
            family="uplift", version="v3.1", measurement_key="x",
            tables=[Table(name="t", path="a.b.c", key="x",
                          metrics=[Metric(name="m", column="c", dtype="double")])],
        )


def test_duplicate_metric_names_within_table_rejected():
    with pytest.raises(ValidationError, match="duplicate metric"):
        Table(
            name="t", path="a.b.c", key="x",
            metrics=[
                Metric(name="m", column="c1", dtype="double"),
                Metric(name="m", column="c2", dtype="double"),
            ],
        )


def test_alias_colliding_with_canonical_in_same_table_rejected():
    with pytest.raises(ValidationError, match="alias .* collides"):
        Table(
            name="t", path="a.b.c", key="x",
            metrics=[
                Metric(name="ate", column="ate", dtype="double"),
                Metric(name="treatment_effect", column="te_col",
                       dtype="double", aliases=["ate"]),
            ],
        )


def test_duplicate_table_names_within_spec_rejected():
    with pytest.raises(ValidationError, match="duplicate table"):
        ModelSpec(
            family="uplift", version="3.1.0", measurement_key="x",
            tables=[
                Table(name="t", path="a.b.c1", key="x",
                      metrics=[Metric(name="m1", column="c", dtype="double")]),
                Table(name="t", path="a.b.c2", key="x",
                      metrics=[Metric(name="m2", column="c", dtype="double")]),
            ],
        )


def test_resolve_metric_returns_table_canonical_and_column():
    spec = _sample_spec()
    tbl, canonical, col = spec.resolve_metric("treatment_effect")
    assert (tbl.name, canonical, col) == ("shared", "treatment_effect", "ate")
    tbl, canonical, col = spec.resolve_metric("te")  # alias
    assert (tbl.name, canonical, col) == ("shared", "treatment_effect", "ate")
    tbl, canonical, col = spec.resolve_metric("heterogeneity_score")
    assert (tbl.name, canonical, col) == ("heterogeneity", "cate_variance", "het_score")


def test_resolve_metric_raises_on_unknown():
    spec = _sample_spec()
    with pytest.raises(KeyError, match="unknown metric"):
        spec.resolve_metric("nope")


def test_resolve_metric_raises_on_cross_table_alias_collision():
    with pytest.raises(ValidationError, match="cross-table"):
        ModelSpec(
            family="uplift", version="3.1.0", measurement_key="x",
            tables=[
                Table(name="a", path="a.b.c1", key="x",
                      metrics=[Metric(name="m", column="c", dtype="double",
                                      aliases=["shared_alias"])]),
                Table(name="b", path="a.b.c2", key="x",
                      metrics=[Metric(name="n", column="c", dtype="double",
                                      aliases=["shared_alias"])]),
            ],
        )
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `pytest tests/test_spec.py -v`
Expected: `ImportError` — `Metric`, `ModelSpec`, `Table` not exported.

- [ ] **Step 3: Implement `spec.py`**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/spec.py`:

```python
"""Pydantic models describing a versioned model result contract."""
from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Delta / Spark canonical dtype names we accept in specs. Kept intentionally
# small; expand as needed.
Dtype = Literal["double", "float", "int", "bigint", "string", "boolean", "timestamp"]

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class Metric(BaseModel):
    """A single metric column exposed under a canonical name.

    - `name` is the consumer-facing canonical name.
    - `column` is the physical column in the result table.
    - `aliases` are former canonical names still queryable for continuity.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    column: str = Field(min_length=1)
    dtype: Dtype
    aliases: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _alias_not_equal_to_name(self) -> "Metric":
        if self.name in self.aliases:
            raise ValueError(f"alias may not equal canonical name {self.name!r}")
        return self


class Table(BaseModel):
    """A physical result table storing one or more metrics."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    path: str = Field(
        min_length=1,
        description="Fully-qualified table identifier, e.g. catalog.schema.table",
    )
    key: str = Field(min_length=1, description="Measurement key column name")
    metrics: tuple[Metric, ...]

    @model_validator(mode="after")
    def _no_duplicate_metric_names_or_alias_collisions(self) -> "Table":
        seen_names: set[str] = set()
        seen_lookup: set[str] = set()  # names + aliases combined
        for m in self.metrics:
            if m.name in seen_names:
                raise ValueError(f"duplicate metric {m.name!r} in table {self.name!r}")
            seen_names.add(m.name)
        # alias/canonical collision detection across metrics in same table
        for m in self.metrics:
            for lookup in (m.name, *m.aliases):
                if lookup in seen_lookup:
                    raise ValueError(
                        f"alias {lookup!r} collides with an existing metric name "
                        f"or alias in table {self.name!r}"
                    )
                seen_lookup.add(lookup)
        return self


class ModelSpec(BaseModel):
    """A versioned contract for one model family's result tables."""

    model_config = ConfigDict(frozen=True)

    family: str = Field(min_length=1)
    version: str
    measurement_key: str = Field(min_length=1)
    tables: tuple[Table, ...]

    @model_validator(mode="after")
    def _version_is_semver(self) -> "ModelSpec":
        if not _SEMVER_RE.match(self.version):
            raise ValueError(
                f"version {self.version!r} must be MAJOR.MINOR.PATCH (e.g. 3.1.0)"
            )
        return self

    @model_validator(mode="after")
    def _no_duplicate_table_names(self) -> "ModelSpec":
        seen: set[str] = set()
        for t in self.tables:
            if t.name in seen:
                raise ValueError(f"duplicate table {t.name!r} in spec")
            seen.add(t.name)
        return self

    @model_validator(mode="after")
    def _no_cross_table_alias_collisions(self) -> "ModelSpec":
        """A metric lookup name (canonical or alias) must be unique across the spec."""
        seen: dict[str, str] = {}  # lookup -> table name
        for t in self.tables:
            for m in t.metrics:
                for lookup in (m.name, *m.aliases):
                    if lookup in seen and seen[lookup] != t.name:
                        raise ValueError(
                            f"cross-table collision: metric lookup {lookup!r} "
                            f"appears in tables {seen[lookup]!r} and {t.name!r}"
                        )
                    seen[lookup] = t.name
        return self

    def resolve_metric(self, lookup: str) -> tuple[Table, str, str]:
        """Resolve a canonical name or alias to (Table, canonical_name, physical_column)."""
        for t in self.tables:
            for m in t.metrics:
                if lookup == m.name or lookup in m.aliases:
                    return t, m.name, m.column
        raise KeyError(f"unknown metric {lookup!r} in spec {self.family}@{self.version}")
```

- [ ] **Step 4: Update package exports**

Replace `/mnt/d/Code/ci_reg_poc/src/registry_sdk/__init__.py`:

```python
"""registry_sdk — model registry SDK."""

from registry_sdk.spec import Metric, ModelSpec, Table

__version__ = "0.1.0"
__all__ = ["Metric", "ModelSpec", "Table", "__version__"]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_spec.py -v`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/registry_sdk/spec.py src/registry_sdk/__init__.py tests/test_spec.py
git commit -m "feat(spec): add ModelSpec/Table/Metric with alias resolution"
```

---

## Task 3: Local Spark session factory + pytest fixtures

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/spark_session.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/conftest.py`
- Delete: `/mnt/d/Code/ci_reg_poc/tests/test_smoke.py` (replaced by conftest + a test using it)
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_spark_session.py`

- [ ] **Step 1: Write failing test for the session factory**

Create `/mnt/d/Code/ci_reg_poc/tests/test_spark_session.py`:

```python
"""Tests for the Spark session factory used by the local store."""
from registry_sdk.spark_session import build_local_spark_session


def test_build_local_spark_session_supports_delta_and_hive(tmp_path):
    spark = build_local_spark_session(
        warehouse_dir=tmp_path / "warehouse",
        metastore_dir=tmp_path / "metastore_db",
        app_name="test_build",
    )
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS t_db")  # DDL for DB: no DataFrame API
        spark.createDataFrame([(1,)], schema="x INT").write.format("delta").saveAsTable(
            "t_db.tt"
        )
        assert spark.table("t_db.tt").count() == 1
    finally:
        spark.stop()
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_spark_session.py -v`
Expected: `ImportError: cannot import name 'build_local_spark_session'`.

- [ ] **Step 3: Implement `spark_session.py`**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/spark_session.py`:

```python
"""Local Spark session factory (Delta + embedded-Derby Hive metastore).

Mirrors the Spark 3.5 surface used in DBR 16.4 so code written against this
session is portable to a Databricks cluster with minimal changes.
"""
from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_local_spark_session(
    *,
    warehouse_dir: Path | str,
    metastore_dir: Path | str,
    app_name: str = "registry_sdk",
    master: str = "local[2]",
) -> SparkSession:
    """Build (or reuse) a local Spark session with Delta + Hive support.

    Each tmp warehouse/metastore pair gives an isolated catalog, which is what
    the per-test `store` fixture wants. Because Spark only allows one active
    session per JVM, callers that need real isolation should reuse the
    session-scoped fixture and just create fresh databases inside it.
    """
    warehouse_dir = Path(warehouse_dir)
    metastore_dir = Path(metastore_dir)
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    metastore_dir.parent.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Small local session — reduce shuffle partitions to keep tests fast.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
```

- [ ] **Step 4: Write `conftest.py` with session + per-test database fixtures**

Create `/mnt/d/Code/ci_reg_poc/tests/conftest.py`:

```python
"""Shared pytest fixtures.

- `spark` (session-scoped): one SparkSession per pytest run. Spark startup is
  slow, so amortize it.
- `registry_db` (function-scoped): a unique database name per test, dropped
  on teardown, so store tests get a clean namespace without restarting Spark.
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from registry_sdk.spark_session import build_local_spark_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    root = tmp_path_factory.mktemp("spark")
    spark = build_local_spark_session(
        warehouse_dir=root / "warehouse",
        metastore_dir=root / "metastore_db",
        app_name="registry_sdk_tests",
    )
    yield spark
    spark.stop()


@pytest.fixture
def registry_db(spark):
    db_name = f"reg_{uuid.uuid4().hex[:12]}"
    spark.sql(f"CREATE DATABASE {db_name}")
    yield db_name
    spark.sql(f"DROP DATABASE {db_name} CASCADE")
```

- [ ] **Step 5: Remove the old smoke test (its coverage is now in the session test)**

Delete `/mnt/d/Code/ci_reg_poc/tests/test_smoke.py`.

- [ ] **Step 6: Run tests to verify green**

Run: `pytest -v -x`
Expected: `test_spark_session.py` passes; spec tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/registry_sdk/spark_session.py tests/conftest.py tests/test_spark_session.py
git rm tests/test_smoke.py
git commit -m "feat(spark): add local Spark session factory + pytest fixtures"
```

---

## Task 4: SpecStore ABC + Registration dataclass

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/__init__.py`
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/base.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_store_base.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_store_base.py`:

```python
"""Tests for the SpecStore ABC + Registration dataclass surface."""
import inspect
from datetime import datetime

import pytest

from registry_sdk import Metric, ModelSpec, Table
from registry_sdk.store.base import Registration, SpecStore, Status


def test_status_values():
    assert set(s.value for s in Status) == {
        "experiment", "challenger", "production", "retired"
    }


def test_registration_dataclass_roundtrips_datetime():
    spec = ModelSpec(
        family="f", version="1.0.0", measurement_key="k",
        tables=[Table(name="t", path="a.b.c", key="k",
                      metrics=[Metric(name="m", column="c", dtype="double")])],
    )
    reg = Registration(
        spec=spec, git_repo="org/repo", git_tag="v1.0.0",
        git_sha="deadbeef", sdk_version="0.1.0",
        registered_by="alice", registered_at=datetime(2026, 1, 1),
    )
    assert reg.family == "f"
    assert reg.version == "1.0.0"


def test_specstore_is_abstract_and_declares_required_methods():
    assert inspect.isabstract(SpecStore)
    for name in (
        "put", "exists", "get", "list_families", "list_versions",
        "promote", "current_status", "by_status", "history",
    ):
        assert hasattr(SpecStore, name), f"SpecStore missing method {name!r}"
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_store_base.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `store/base.py`**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/base.py`:

```python
"""Storage abstraction for the model registry.

Two conceptual tables:
- `registrations`: immutable, one row per (family, version).
- `status_log`: append-only status events per (family, version).

Concrete implementations must guarantee that promote() writes are atomic;
in particular, a production promotion writes both the incoming version's
`production` event and the outgoing version's `retired` event in one commit.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from registry_sdk.spec import ModelSpec


class Status(str, Enum):
    EXPERIMENT = "experiment"
    CHALLENGER = "challenger"
    PRODUCTION = "production"
    RETIRED = "retired"


@dataclass(frozen=True)
class Registration:
    spec: ModelSpec
    git_repo: str
    git_tag: str
    git_sha: str
    sdk_version: str
    registered_by: str
    registered_at: datetime

    @property
    def family(self) -> str:
        return self.spec.family

    @property
    def version(self) -> str:
        return self.spec.version


@dataclass(frozen=True)
class StatusEvent:
    event_id: str
    family: str
    version: str
    status: Status
    effective_from: datetime
    assigned_by: str
    note: str


class SpecStore(ABC):
    """Two-table registry storage."""

    # --- registrations -------------------------------------------------------

    @abstractmethod
    def put(
        self,
        spec: ModelSpec,
        *,
        git_repo: str,
        git_tag: str,
        git_sha: str,
        registered_by: str,
        sdk_version: str,
    ) -> Registration:
        """Insert a registration and its initial `experiment` status event.

        Raises `KeyError` if (family, version) already exists (callers use
        `exists` first when they want to expose a --force flag)."""

    @abstractmethod
    def exists(self, family: str, version: str) -> bool: ...

    @abstractmethod
    def get(self, family: str, version: str) -> Registration: ...

    @abstractmethod
    def list_families(self) -> list[str]: ...

    @abstractmethod
    def list_versions(self, family: str) -> list[str]: ...

    # --- status --------------------------------------------------------------

    @abstractmethod
    def promote(
        self,
        family: str,
        version: str,
        status: Status,
        *,
        assigned_by: str,
        note: str = "",
        reactivate: bool = False,
    ) -> None:
        """Append a status event. Enforces:

        - `production` promotion is atomic with retirement of the current prod.
        - Un-retiring (retired -> anything) requires `reactivate=True`.
        """

    @abstractmethod
    def current_status(
        self,
        family: str,
        version: str,
        *,
        as_of: datetime | None = None,
    ) -> Status: ...

    @abstractmethod
    def by_status(
        self,
        family: str,
        status: Status | list[Status],
        *,
        as_of: datetime | None = None,
    ) -> list[Registration]: ...

    @abstractmethod
    def history(self, family: str, version: str) -> list[StatusEvent]: ...
```

- [ ] **Step 4: Add store package init**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/__init__.py`:

```python
"""Registry storage backends."""

from registry_sdk.store.base import Registration, SpecStore, Status, StatusEvent

__all__ = ["Registration", "SpecStore", "Status", "StatusEvent"]
```

- [ ] **Step 5: Verify tests pass**

Run: `pytest tests/test_store_base.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/registry_sdk/store tests/test_store_base.py
git commit -m "feat(store): add SpecStore ABC + Registration/StatusEvent dataclasses"
```

---

## Task 5: SparkHiveSpecStore — schema init

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_store_init.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_store_init.py`:

```python
"""Tests for SparkHiveSpecStore schema initialization."""
from registry_sdk.store.spark_hive import SparkHiveSpecStore


def test_ensure_tables_creates_both_tables(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    tables = {t.name for t in spark.catalog.listTables(registry_db)}
    assert {"registrations", "status_log"}.issubset(tables)


def test_ensure_tables_is_idempotent(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    store.ensure_tables()  # second call must not raise
    tables = {t.name for t in spark.catalog.listTables(registry_db)}
    assert {"registrations", "status_log"}.issubset(tables)


def test_registrations_schema_matches_spec(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    schema = {
        f.name: f.dataType.simpleString()
        for f in spark.table(f"{registry_db}.registrations").schema.fields
    }
    assert schema == {
        "family": "string",
        "version": "string",
        "spec_json": "string",
        "git_repo": "string",
        "git_tag": "string",
        "git_sha": "string",
        "sdk_version": "string",
        "registered_by": "string",
        "registered_at": "timestamp",
    }


def test_status_log_schema_matches_spec(spark, registry_db):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    schema = {
        f.name: f.dataType.simpleString()
        for f in spark.table(f"{registry_db}.status_log").schema.fields
    }
    assert schema == {
        "event_id": "string",
        "family": "string",
        "version": "string",
        "status": "string",
        "effective_from": "timestamp",
        "assigned_by": "string",
        "note": "string",
    }
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_store_init.py -v`
Expected: `ModuleNotFoundError: registry_sdk.store.spark_hive`.

- [ ] **Step 3: Implement schema init**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`:

```python
"""Delta-backed SpecStore.

Uses the PySpark DataFrame API and Delta's Python builders for all
reads/writes. SQL is used only for `CREATE DATABASE` (no DataFrame
equivalent). Local dev uses embedded-Derby Hive; on Databricks the same code
targets Unity Catalog by pointing `database` at a UC schema (e.g.
`main.registry`).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from delta.tables import DeltaTable

from registry_sdk.store.base import (
    Registration,
    SpecStore,
    Status,
    StatusEvent,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class SparkHiveSpecStore(SpecStore):
    spark: "SparkSession"
    database: str

    def ensure_tables(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        (
            DeltaTable.createIfNotExists(self.spark)
            .tableName(f"{self.database}.registrations")
            .addColumn("family", "STRING")
            .addColumn("version", "STRING")
            .addColumn("spec_json", "STRING")
            .addColumn("git_repo", "STRING")
            .addColumn("git_tag", "STRING")
            .addColumn("git_sha", "STRING")
            .addColumn("sdk_version", "STRING")
            .addColumn("registered_by", "STRING")
            .addColumn("registered_at", "TIMESTAMP")
            .execute()
        )
        (
            DeltaTable.createIfNotExists(self.spark)
            .tableName(f"{self.database}.status_log")
            .addColumn("event_id", "STRING")
            .addColumn("family", "STRING")
            .addColumn("version", "STRING")
            .addColumn("status", "STRING")
            .addColumn("effective_from", "TIMESTAMP")
            .addColumn("assigned_by", "STRING")
            .addColumn("note", "STRING")
            .execute()
        )

    # ---- SpecStore abstract methods: stubs; filled in by later tasks --------

    def put(self, spec, *, git_repo, git_tag, git_sha, registered_by, sdk_version):
        raise NotImplementedError

    def exists(self, family, version):
        raise NotImplementedError

    def get(self, family, version):
        raise NotImplementedError

    def list_families(self):
        raise NotImplementedError

    def list_versions(self, family):
        raise NotImplementedError

    def promote(self, family, version, status, *, assigned_by, note="", reactivate=False):
        raise NotImplementedError

    def current_status(self, family, version, *, as_of=None):
        raise NotImplementedError

    def by_status(self, family, status, *, as_of=None):
        raise NotImplementedError

    def history(self, family, version):
        raise NotImplementedError
```

- [ ] **Step 4: Run init tests to verify green**

Run: `pytest tests/test_store_init.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/registry_sdk/store/spark_hive.py tests/test_store_init.py
git commit -m "feat(store): create Delta-backed registrations + status_log tables"
```

---

## Task 6: SparkHiveSpecStore — put / exists / get

**Files:**
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_store_put_get.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/fixtures.py`

- [ ] **Step 1: Add shared spec fixture module**

Create `/mnt/d/Code/ci_reg_poc/tests/fixtures.py`:

```python
"""Helpers to build sample ModelSpec objects for tests."""
from registry_sdk import Metric, ModelSpec, Table


def sample_spec(family: str = "uplift", version: str = "3.1.0") -> ModelSpec:
    return ModelSpec(
        family=family,
        version=version,
        measurement_key="experiment_id",
        tables=[
            Table(
                name="shared",
                path=f"analytics.{family}.shared_v{version.split('.')[0]}",
                key="experiment_id",
                metrics=[
                    Metric(name="treatment_effect", column="ate",
                           dtype="double", aliases=["te"]),
                    Metric(name="ci_lower", column="ci_lo", dtype="double"),
                    Metric(name="ci_upper", column="ci_hi", dtype="double"),
                ],
            ),
        ],
    )
```

- [ ] **Step 2: Write failing tests for put / exists / get**

Create `/mnt/d/Code/ci_reg_poc/tests/test_store_put_get.py`:

```python
"""Tests for SparkHiveSpecStore.put / exists / get."""
import pytest

from registry_sdk.store.base import Status
from registry_sdk.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _make_store(spark, db):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    return store


def _put(store, spec):
    return store.put(
        spec,
        git_repo="org/uplift-model",
        git_tag=f"v{spec.version}",
        git_sha="a" * 40,
        registered_by="alice",
        sdk_version="0.1.0",
    )


def test_put_inserts_registration_and_initial_experiment_event(spark, registry_db):
    store = _make_store(spark, registry_db)
    spec = sample_spec()
    reg = _put(store, spec)
    assert reg.family == "uplift" and reg.version == "3.1.0"

    reg_rows = spark.table(f"{registry_db}.registrations").collect()
    assert len(reg_rows) == 1
    assert reg_rows[0]["family"] == "uplift"

    status_rows = spark.table(f"{registry_db}.status_log").collect()
    assert len(status_rows) == 1
    assert status_rows[0]["status"] == Status.EXPERIMENT.value


def test_put_rejects_duplicate_family_version(spark, registry_db):
    store = _make_store(spark, registry_db)
    spec = sample_spec()
    _put(store, spec)
    with pytest.raises(KeyError, match="already registered"):
        _put(store, spec)


def test_exists_and_get(spark, registry_db):
    store = _make_store(spark, registry_db)
    spec = sample_spec()
    assert not store.exists("uplift", "3.1.0")
    _put(store, spec)
    assert store.exists("uplift", "3.1.0")
    fetched = store.get("uplift", "3.1.0")
    assert fetched.spec == spec
    assert fetched.git_repo == "org/uplift-model"
    assert fetched.git_sha == "a" * 40


def test_get_raises_when_missing(spark, registry_db):
    store = _make_store(spark, registry_db)
    with pytest.raises(KeyError, match="not found"):
        store.get("uplift", "9.9.9")
```

- [ ] **Step 3: Run tests to confirm failure**

Run: `pytest tests/test_store_put_get.py -v`
Expected: FAIL — `put` raises `NotImplementedError`.

- [ ] **Step 4: Implement put / exists / get**

Modify `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`. Replace the whole `put`, `exists`, `get` stubs with:

```python
    def put(self, spec, *, git_repo, git_tag, git_sha, registered_by, sdk_version):
        if self.exists(spec.family, spec.version):
            raise KeyError(
                f"{spec.family}@{spec.version} already registered"
            )
        now = datetime.utcnow()
        reg_row = (
            spec.family, spec.version, spec.model_dump_json(),
            git_repo, git_tag, git_sha, sdk_version, registered_by, now,
        )
        (
            self.spark.createDataFrame([reg_row], schema=_REG_SCHEMA)
            .write.format("delta").mode("append")
            .saveAsTable(f"{self.database}.registrations")
        )
        # Initial status event: experiment.
        self._append_status_events([
            _new_status_event(spec.family, spec.version, Status.EXPERIMENT,
                              registered_by, "initial registration", now),
        ])
        return Registration(
            spec=spec, git_repo=git_repo, git_tag=git_tag, git_sha=git_sha,
            sdk_version=sdk_version, registered_by=registered_by,
            registered_at=now,
        )

    def exists(self, family, version):
        return (
            self.spark.table(f"{self.database}.registrations")
            .filter((F.col("family") == family) & (F.col("version") == version))
            .limit(1).take(1)
        ) != []

    def get(self, family, version):
        rows = (
            self.spark.table(f"{self.database}.registrations")
            .filter((F.col("family") == family) & (F.col("version") == version))
            .collect()
        )
        if not rows:
            raise KeyError(f"{family}@{version} not found")
        r = rows[0]
        spec = ModelSpec.model_validate_json(r["spec_json"])
        return Registration(
            spec=spec, git_repo=r["git_repo"], git_tag=r["git_tag"],
            git_sha=r["git_sha"], sdk_version=r["sdk_version"],
            registered_by=r["registered_by"], registered_at=r["registered_at"],
        )
```

Add these imports/helpers at the top of `spark_hive.py` (below the existing imports):

```python
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from registry_sdk.spec import ModelSpec


# Explicit write schemas — createDataFrame infers types from the tuples,
# but we want TIMESTAMP (not string) and non-nullable columns.
_REG_SCHEMA = StructType([
    StructField("family", StringType(), nullable=False),
    StructField("version", StringType(), nullable=False),
    StructField("spec_json", StringType(), nullable=False),
    StructField("git_repo", StringType(), nullable=False),
    StructField("git_tag", StringType(), nullable=False),
    StructField("git_sha", StringType(), nullable=False),
    StructField("sdk_version", StringType(), nullable=False),
    StructField("registered_by", StringType(), nullable=False),
    StructField("registered_at", TimestampType(), nullable=False),
])

_STATUS_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("family", StringType(), nullable=False),
    StructField("version", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("effective_from", TimestampType(), nullable=False),
    StructField("assigned_by", StringType(), nullable=False),
    StructField("note", StringType(), nullable=False),
])


def _new_status_event(family, version, status, assigned_by, note, at):
    return (str(uuid.uuid4()), family, version, status.value, at, assigned_by, note)
```

Add the shared status-append helper as a method on the class (place it just above `promote`):

```python
    def _append_status_events(self, rows):
        df = self.spark.createDataFrame(list(rows), schema=_STATUS_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(
            f"{self.database}.status_log"
        )
```

- [ ] **Step 5: Run tests to verify green**

Run: `pytest tests/test_store_put_get.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/registry_sdk/store/spark_hive.py tests/test_store_put_get.py tests/fixtures.py
git commit -m "feat(store): implement put/exists/get with initial experiment event"
```

---

## Task 7: SparkHiveSpecStore — status queries

**Files:**
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_store_status.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_store_status.py`:

```python
"""Tests for status query methods on SparkHiveSpecStore."""
from datetime import datetime, timedelta

import pytest

from registry_sdk.store.base import Status
from registry_sdk.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _store_with(spark, db, *versions):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for v in versions:
        store.put(
            sample_spec(version=v),
            git_repo="org/uplift-model", git_tag=f"v{v}",
            git_sha="a" * 40, registered_by="alice", sdk_version="0.1.0",
        )
    return store


def test_list_families_and_versions(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0", "3.1.0")
    assert store.list_families() == ["uplift"]
    assert sorted(store.list_versions("uplift")) == ["3.0.0", "3.1.0"]


def test_current_status_starts_as_experiment(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    assert store.current_status("uplift", "3.0.0") == Status.EXPERIMENT


def test_by_status_returns_matching_registrations(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0", "3.1.0")
    regs = store.by_status("uplift", Status.EXPERIMENT)
    assert sorted(r.version for r in regs) == ["3.0.0", "3.1.0"]
    assert store.by_status("uplift", Status.PRODUCTION) == []


def test_by_status_accepts_list_of_statuses(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    regs = store.by_status("uplift", [Status.EXPERIMENT, Status.CHALLENGER])
    assert len(regs) == 1


def test_history_returns_events_in_order(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    events = store.history("uplift", "3.0.0")
    assert len(events) == 1
    assert events[0].status == Status.EXPERIMENT


def test_current_status_missing_version_raises(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    with pytest.raises(KeyError):
        store.current_status("uplift", "9.9.9")


def test_current_status_respects_as_of(spark, registry_db):
    """Point-in-time query: an event added after `as_of` must not be visible."""
    store = _store_with(spark, registry_db, "3.0.0")
    before = datetime.utcnow() - timedelta(days=365)
    # future event: simulate by appending directly through helper
    import uuid
    future_row = (
        str(uuid.uuid4()), "uplift", "3.0.0", Status.PRODUCTION.value,
        datetime.utcnow() + timedelta(days=1), "alice", "",
    )
    from registry_sdk.store.spark_hive import _STATUS_SCHEMA
    spark.createDataFrame([future_row], schema=_STATUS_SCHEMA).write.format(
        "delta"
    ).mode("append").saveAsTable(f"{registry_db}.status_log")

    # as_of before any event -> KeyError
    with pytest.raises(KeyError):
        store.current_status("uplift", "3.0.0", as_of=before)
    # as_of now -> experiment (future event excluded)
    assert store.current_status(
        "uplift", "3.0.0", as_of=datetime.utcnow(),
    ) == Status.EXPERIMENT
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_store_status.py -v`
Expected: FAIL — methods raise `NotImplementedError`.

- [ ] **Step 3: Implement status query methods**

In `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`, replace the `list_families`, `list_versions`, `current_status`, `by_status`, `history` stubs:

```python
    def list_families(self):
        rows = (
            self.spark.table(f"{self.database}.registrations")
            .select("family").distinct().orderBy("family")
            .collect()
        )
        return [r["family"] for r in rows]

    def list_versions(self, family):
        rows = (
            self.spark.table(f"{self.database}.registrations")
            .filter(F.col("family") == family)
            .select("version").distinct().orderBy("version")
            .collect()
        )
        return [r["version"] for r in rows]

    def current_status(self, family, version, *, as_of=None):
        df = (
            self.spark.table(f"{self.database}.status_log")
            .filter((F.col("family") == family) & (F.col("version") == version))
        )
        if as_of is not None:
            df = df.filter(F.col("effective_from") <= F.lit(as_of))
        rows = df.orderBy(F.col("effective_from").desc()).limit(1).collect()
        if not rows:
            raise KeyError(f"{family}@{version} has no status events at or before as_of")
        return Status(rows[0]["status"])

    def by_status(self, family, status, *, as_of=None):
        wanted = [status] if isinstance(status, Status) else list(status)
        wanted_set = {s.value for s in wanted}
        # Get all (family, version) with current status matching.
        out = []
        for v in self.list_versions(family):
            try:
                cur = self.current_status(family, v, as_of=as_of)
            except KeyError:
                continue
            if cur.value in wanted_set:
                out.append(self.get(family, v))
        return out

    def history(self, family, version):
        rows = (
            self.spark.table(f"{self.database}.status_log")
            .filter((F.col("family") == family) & (F.col("version") == version))
            .orderBy(F.col("effective_from").asc())
            .collect()
        )
        return [
            StatusEvent(
                event_id=r["event_id"], family=r["family"], version=r["version"],
                status=Status(r["status"]), effective_from=r["effective_from"],
                assigned_by=r["assigned_by"], note=r["note"] or "",
            )
            for r in rows
        ]
```

- [ ] **Step 4: Run tests to verify green**

Run: `pytest tests/test_store_status.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/registry_sdk/store/spark_hive.py tests/test_store_status.py
git commit -m "feat(store): status queries (list/current/by_status/history)"
```

---

## Task 8: SparkHiveSpecStore — promote (atomic production swap)

**Files:**
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/spark_hive.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_store_promote.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_store_promote.py`:

```python
"""Tests for promote() including atomic production swap and reactivate guard."""
import pytest

from registry_sdk.store.base import Status
from registry_sdk.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _store_with(spark, db, *versions):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for v in versions:
        store.put(
            sample_spec(version=v),
            git_repo="org/uplift-model", git_tag=f"v{v}",
            git_sha="a" * 40, registered_by="alice", sdk_version="0.1.0",
        )
    return store


def test_promote_to_challenger(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob")
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER


def test_promote_unknown_version_raises(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    with pytest.raises(KeyError, match="not registered"):
        store.promote("uplift", "9.9.9", Status.CHALLENGER, assigned_by="bob")


def test_promote_to_production_when_none_current(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    prods = store.by_status("uplift", Status.PRODUCTION)
    assert [r.version for r in prods] == ["3.0.0"]


def test_promote_to_production_retires_current_prod_atomically(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0", "3.1.0")
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote(
        "uplift", "3.1.0", Status.PRODUCTION, assigned_by="bob",
        note="passed 2-week challenger review",
    )
    prods = store.by_status("uplift", Status.PRODUCTION)
    assert [r.version for r in prods] == ["3.1.0"]
    assert store.current_status("uplift", "3.0.0") == Status.RETIRED
    # And the retirement was written in the same batch (same effective_from as promote).
    events_30 = [e for e in store.history("uplift", "3.0.0")
                 if e.status == Status.RETIRED]
    events_31 = [e for e in store.history("uplift", "3.1.0")
                 if e.status == Status.PRODUCTION]
    assert events_30 and events_31
    assert events_30[0].effective_from == events_31[0].effective_from


def test_unretire_requires_reactivate_flag(spark, registry_db):
    store = _store_with(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "3.0.0", Status.RETIRED, assigned_by="bob")
    with pytest.raises(ValueError, match="reactivate"):
        store.promote("uplift", "3.0.0", Status.CHALLENGER, assigned_by="bob")
    # With flag, it proceeds.
    store.promote(
        "uplift", "3.0.0", Status.CHALLENGER,
        assigned_by="bob", reactivate=True,
    )
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_store_promote.py -v`
Expected: FAIL — `promote` raises `NotImplementedError`.

- [ ] **Step 3: Implement `promote`**

Replace the `promote` stub in `spark_hive.py`:

```python
    def promote(self, family, version, status, *, assigned_by, note="", reactivate=False):
        if not self.exists(family, version):
            raise KeyError(f"{family}@{version} is not registered")

        current = self.current_status(family, version)
        if current == Status.RETIRED and not reactivate:
            raise ValueError(
                f"{family}@{version} is retired; pass reactivate=True to un-retire"
            )

        now = datetime.utcnow()
        events = [
            _new_status_event(family, version, status, assigned_by, note, now),
        ]

        # Atomic production swap: any current prod in the family must be
        # retired in the same commit as the new prod's promotion.
        if status == Status.PRODUCTION:
            for reg in self.by_status(family, Status.PRODUCTION):
                if reg.version == version:
                    continue
                events.append(
                    _new_status_event(
                        family, reg.version, Status.RETIRED, assigned_by,
                        f"auto-retired on promotion of {version} to production",
                        now,
                    )
                )
        self._append_status_events(events)
```

- [ ] **Step 4: Run tests to verify green**

Run: `pytest tests/test_store_promote.py -v`
Expected: PASS.

- [ ] **Step 5: Run full suite**

Run: `pytest -x -q`
Expected: All previous tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/registry_sdk/store/spark_hive.py tests/test_store_promote.py
git commit -m "feat(store): atomic production promotion with retire-in-same-commit"
```

---

## Task 9: UC validation module (missing-vs-drift)

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/validation.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_validation.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_validation.py`:

```python
"""Tests for validate_against_uc — must warn on missing tables and error on drift."""
from registry_sdk import Metric, ModelSpec, Table
from registry_sdk.validation import validate_against_uc


def _spec(path: str) -> ModelSpec:
    return ModelSpec(
        family="uplift", version="3.1.0", measurement_key="k",
        tables=[Table(name="t", path=path, key="k", metrics=[
            Metric(name="ate", column="ate", dtype="double"),
            Metric(name="ci_lower", column="ci_lo", dtype="double"),
        ])],
    )


def test_missing_table_produces_warning_not_error(spark, registry_db):
    spec = _spec(f"{registry_db}.does_not_exist")
    report = validate_against_uc(spec, spark=spark)
    assert not report.has_errors
    assert report.has_warnings
    assert "does not exist" in "\n".join(report.warnings)


def _write_empty(spark, name, schema):
    """Create an empty Delta table with the given schema (test helper)."""
    spark.createDataFrame([], schema=schema).write.format("delta").saveAsTable(name)


def test_matching_table_produces_no_findings(spark, registry_db):
    _write_empty(spark, f"{registry_db}.results", "k STRING, ate DOUBLE, ci_lo DOUBLE")
    report = validate_against_uc(_spec(f"{registry_db}.results"), spark=spark)
    assert not report.has_errors and not report.has_warnings


def test_missing_column_produces_error(spark, registry_db):
    _write_empty(spark, f"{registry_db}.results", "k STRING, ate DOUBLE")
    report = validate_against_uc(_spec(f"{registry_db}.results"), spark=spark)
    assert report.has_errors
    assert any("ci_lo" in e for e in report.errors)


def test_dtype_mismatch_produces_error(spark, registry_db):
    _write_empty(spark, f"{registry_db}.results", "k STRING, ate STRING, ci_lo DOUBLE")
    report = validate_against_uc(_spec(f"{registry_db}.results"), spark=spark)
    assert report.has_errors
    joined = "\n".join(report.errors)
    assert "ate" in joined and "double" in joined and "string" in joined
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_validation.py -v`
Expected: `ImportError` — module doesn't exist.

- [ ] **Step 3: Implement `validation.py`**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/validation.py`:

```python
"""Validate a ModelSpec's declared tables/columns against the live catalog.

Distinguishes two failure modes:
- Table missing: warn (expected on first registration before the pipeline runs)
- Table present but column missing or dtype mismatch: error (real drift)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from registry_sdk.spec import ModelSpec

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)

    def format(self) -> str:
        parts = []
        if self.errors:
            parts.append("Errors:\n  " + "\n  ".join(self.errors))
        if self.warnings:
            parts.append("Warnings:\n  " + "\n  ".join(self.warnings))
        return "\n".join(parts)

    def format_warnings(self) -> str:
        return "Warnings:\n  " + "\n  ".join(self.warnings)


def validate_against_uc(spec: ModelSpec, *, spark: "SparkSession") -> ValidationReport:
    report = ValidationReport()
    for table in spec.tables:
        try:
            actual = {
                f.name: f.dataType.simpleString()
                for f in spark.table(table.path).schema.fields
            }
        except Exception:
            report.warnings.append(
                f"{table.path} does not exist yet (first registration?)"
            )
            continue
        for m in table.metrics:
            if m.column not in actual:
                report.errors.append(
                    f"{table.path} missing column {m.column!r} "
                    f"(declared for metric {m.name!r})"
                )
            elif actual[m.column] != m.dtype:
                report.errors.append(
                    f"{table.path}.{m.column}: spec says {m.dtype}, "
                    f"table has {actual[m.column]}"
                )
    return report
```

- [ ] **Step 4: Run tests to verify green**

Run: `pytest tests/test_validation.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/registry_sdk/validation.py tests/test_validation.py
git commit -m "feat(validation): validate specs vs catalog with warn-vs-error split"
```

---

## Task 10: Store factory (`get_store`) + registration CLI

**Files:**
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/__init__.py`
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/cli.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_cli_register.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_cli_register.py`:

```python
"""Tests for the `registry-sdk register` CLI."""
import textwrap

from click.testing import CliRunner

from registry_sdk.cli import cli
from registry_sdk.store.spark_hive import SparkHiveSpecStore


def _write_spec(tmp_path, family="uplift", version="3.1.0", table_path=None):
    """Emit a model_spec.py file that defines a top-level `spec` variable."""
    table_path = table_path or f"nonexistent_ns.{family}.results"
    spec_py = tmp_path / "model_spec.py"
    spec_py.write_text(textwrap.dedent(f"""
        from registry_sdk import Metric, ModelSpec, Table
        spec = ModelSpec(
            family="{family}", version="{version}", measurement_key="k",
            tables=[Table(name="t", path="{table_path}", key="k",
                          metrics=[Metric(name="ate", column="ate", dtype="double")])],
        )
    """))
    return spec_py


def _invoke_register(spark, store, spec_py, extra=()):
    """Inject the shared Spark session + store via Click's ctx.obj."""
    runner = CliRunner()
    return runner.invoke(
        cli, [
            "register",
            "--spec-path", str(spec_py),
            "--git-repo", "org/uplift-model",
            "--git-tag", "v3.1.0",
            "--git-sha", "a" * 40,
            "--registered-by", "alice",
            *extra,
        ],
        obj={"spark": spark, "store": store},
    )


def test_register_happy_path_writes_registration(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    spec_py = _write_spec(tmp_path)
    result = _invoke_register(spark, store, spec_py)
    assert result.exit_code == 0, result.output
    assert store.exists("uplift", "3.1.0")


def test_register_rejects_git_tag_mismatch(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    spec_py = _write_spec(tmp_path, version="3.2.0")  # spec says 3.2.0
    result = _invoke_register(spark, store, spec_py)  # CLI passes tag v3.1.0
    assert result.exit_code != 0
    assert "does not match spec version" in result.output


def test_register_is_idempotent_without_force(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    spec_py = _write_spec(tmp_path)
    r1 = _invoke_register(spark, store, spec_py)
    assert r1.exit_code == 0
    r2 = _invoke_register(spark, store, spec_py)
    assert r2.exit_code != 0
    assert "already registered" in r2.output


def test_register_fails_on_catalog_drift(spark, registry_db, tmp_path):
    store = SparkHiveSpecStore(spark=spark, database=registry_db)
    store.ensure_tables()
    # Create a table with the wrong dtype for 'ate'.
    spark.createDataFrame([], "k STRING, ate STRING").write.format("delta").saveAsTable(
        f"{registry_db}.results"
    )
    spec_py = _write_spec(tmp_path, table_path=f"{registry_db}.results")
    result = _invoke_register(spark, store, spec_py)
    assert result.exit_code != 0
    assert "UC validation failed" in result.output
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_cli_register.py -v`
Expected: `ImportError` on `registry_sdk.cli`.

- [ ] **Step 3: Implement store factory**

Replace `/mnt/d/Code/ci_reg_poc/src/registry_sdk/store/__init__.py`:

```python
"""Registry storage backends and factory."""
from __future__ import annotations

import json
import os

from registry_sdk.store.base import Registration, SpecStore, Status, StatusEvent
from registry_sdk.store.spark_hive import SparkHiveSpecStore

__all__ = [
    "Registration", "SpecStore", "Status", "StatusEvent",
    "SparkHiveSpecStore", "get_store",
]


def get_store(spark=None) -> SpecStore:
    """Build the configured SpecStore.

    Reads `REGISTRY_STORE_CONFIG` (JSON) from the environment, e.g.
        {"backend": "spark_hive", "database": "main.registry"}

    Defaults to `{"backend": "spark_hive", "database": "registry"}`.
    """
    raw = os.environ.get(
        "REGISTRY_STORE_CONFIG",
        '{"backend": "spark_hive", "database": "registry"}',
    )
    cfg = json.loads(raw)
    backend = cfg.get("backend", "spark_hive")
    if backend != "spark_hive":
        raise ValueError(f"unsupported backend {backend!r}")
    if spark is None:
        raise ValueError("get_store requires a SparkSession for the spark_hive backend")
    store = SparkHiveSpecStore(spark=spark, database=cfg["database"])
    store.ensure_tables()
    return store
```

- [ ] **Step 4: Implement `cli.py` with `register` subcommand**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/cli.py`:

```python
"""registry-sdk CLI: register and promote."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import click
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from registry_sdk import ModelSpec, __version__
from registry_sdk.spark_session import build_local_spark_session
from registry_sdk.store import get_store
from registry_sdk.validation import validate_against_uc


def _load_spec(spec_path: Path) -> ModelSpec:
    module_spec = importlib.util.spec_from_file_location("model_spec", spec_path)
    if module_spec is None or module_spec.loader is None:
        raise click.ClickException(f"cannot load spec module from {spec_path}")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules["model_spec"] = module
    module_spec.loader.exec_module(module)
    if not hasattr(module, "spec"):
        raise click.ClickException(
            f"{spec_path} must define a top-level `spec` variable"
        )
    if not isinstance(module.spec, ModelSpec):
        raise click.ClickException(
            f"`spec` in {spec_path} must be a ModelSpec, got {type(module.spec).__name__}"
        )
    return module.spec


@click.group()
@click.version_option(__version__)
@click.option(
    "--warehouse-dir", type=click.Path(path_type=Path),
    default=Path.home() / ".registry_sdk" / "warehouse",
    envvar="REGISTRY_WAREHOUSE_DIR", show_default=True,
)
@click.option(
    "--metastore-dir", type=click.Path(path_type=Path),
    default=Path.home() / ".registry_sdk" / "metastore_db",
    envvar="REGISTRY_METASTORE_DIR", show_default=True,
)
@click.pass_context
def cli(ctx: click.Context, warehouse_dir: Path, metastore_dir: Path) -> None:
    """Model registry SDK CLI.

    Tests can inject a pre-built Spark session and store by passing
    `obj={"spark": ..., "store": ...}` to `CliRunner.invoke`, in which case
    the flags are ignored.
    """
    ctx.ensure_object(dict)
    if "spark" not in ctx.obj:
        ctx.obj["spark"] = build_local_spark_session(
            warehouse_dir=warehouse_dir,
            metastore_dir=metastore_dir,
            app_name="registry_sdk_cli",
        )
    if "store" not in ctx.obj:
        ctx.obj["store"] = get_store(ctx.obj["spark"])


@cli.command()
@click.option("--spec-path", type=click.Path(exists=True, path_type=Path),
              default=Path("model_spec.py"), show_default=True)
@click.option("--git-repo", required=True, envvar="GITHUB_REPOSITORY")
@click.option("--git-tag", required=True, envvar="GITHUB_REF_NAME")
@click.option("--git-sha", required=True, envvar="GITHUB_SHA")
@click.option("--registered-by", required=True, envvar="GITHUB_ACTOR")
@click.option("--force", is_flag=True,
              help="Overwrite an existing registration for this (family, version).")
@click.pass_context
def register(ctx, spec_path, git_repo, git_tag, git_sha, registered_by, force):
    """Register a ModelSpec from a local model_spec.py."""
    spark, store = ctx.obj["spark"], ctx.obj["store"]

    spec = _load_spec(spec_path)

    expected_tag = f"v{spec.version}"
    if git_tag != expected_tag:
        raise click.ClickException(
            f"Git tag {git_tag!r} does not match spec version (expected {expected_tag!r})"
        )

    report = validate_against_uc(spec, spark=spark)
    if report.has_errors:
        click.echo(report.format(), err=True)
        raise click.ClickException("UC validation failed")
    if report.has_warnings:
        click.echo(report.format_warnings(), err=True)

    if store.exists(spec.family, spec.version) and not force:
        raise click.ClickException(
            f"{spec.family}@{spec.version} already registered. Use --force to overwrite."
        )
    if store.exists(spec.family, spec.version) and force:
        # --force is best-effort for the POC: SparkHiveSpecStore.put refuses
        # duplicates, so drop the prior registration row first. Idempotency
        # policy can tighten later.
        click.echo(f"warning: --force overwrite of {spec.family}@{spec.version}", err=True)
        DeltaTable.forName(spark, f"{store.database}.registrations").delete(
            (F.col("family") == spec.family) & (F.col("version") == spec.version)
        )

    reg = store.put(
        spec, git_repo=git_repo, git_tag=git_tag, git_sha=git_sha,
        registered_by=registered_by, sdk_version=__version__,
    )
    click.echo(f"Registered: {reg.family}@{reg.version} (sha={reg.git_sha[:8]})")
```

- [ ] **Step 5: Run tests to verify green**

Run: `pytest tests/test_cli_register.py -v`
Expected: PASS.

- [ ] **Step 6: Manual sanity check**

Run: `registry-sdk --help`
Expected: shows `register` subcommand.

- [ ] **Step 7: Commit**

```bash
git add src/registry_sdk/store/__init__.py src/registry_sdk/cli.py tests/test_cli_register.py
git commit -m "feat(cli): registry-sdk register with tag-match + UC validation + idempotency"
```

---

## Task 11: Promotion CLI

**Files:**
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/cli.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_cli_promote.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_cli_promote.py`:

```python
"""Tests for the `registry-sdk promote` CLI."""
from click.testing import CliRunner

from registry_sdk.cli import cli
from registry_sdk.store.base import Status
from registry_sdk.store.spark_hive import SparkHiveSpecStore
from tests.fixtures import sample_spec


def _seed(spark, db, *versions):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for v in versions:
        store.put(
            sample_spec(version=v),
            git_repo="org/uplift-model", git_tag=f"v{v}",
            git_sha="a" * 40, registered_by="alice", sdk_version="0.1.0",
        )
    return store


def _invoke_promote(spark, store, args):
    runner = CliRunner()
    return runner.invoke(
        cli, ["promote", *args], obj={"spark": spark, "store": store},
    )


def test_promote_to_challenger(spark, registry_db):
    store = _seed(spark, registry_db, "3.0.0")
    result = _invoke_promote(spark, store, [
        "--family", "uplift", "--version", "3.0.0",
        "--status", "challenger", "--assigned-by", "bob",
    ])
    assert result.exit_code == 0, result.output
    assert store.current_status("uplift", "3.0.0") == Status.CHALLENGER


def test_promote_to_production_atomically_retires_prior(spark, registry_db):
    store = _seed(spark, registry_db, "3.0.0", "3.1.0")
    _invoke_promote(spark, store, [
        "--family", "uplift", "--version", "3.0.0",
        "--status", "production", "--assigned-by", "bob",
    ])
    result = _invoke_promote(spark, store, [
        "--family", "uplift", "--version", "3.1.0",
        "--status", "production", "--assigned-by", "bob",
        "--note", "graduated",
    ])
    assert result.exit_code == 0, result.output
    prods = [r.version for r in store.by_status("uplift", Status.PRODUCTION)]
    assert prods == ["3.1.0"]


def test_promote_from_retired_requires_reactivate_flag(spark, registry_db):
    store = _seed(spark, registry_db, "3.0.0")
    store.promote("uplift", "3.0.0", Status.RETIRED, assigned_by="bob")
    result = _invoke_promote(spark, store, [
        "--family", "uplift", "--version", "3.0.0",
        "--status", "challenger", "--assigned-by", "bob",
    ])
    assert result.exit_code != 0
    assert "reactivate" in result.output
    ok = _invoke_promote(spark, store, [
        "--family", "uplift", "--version", "3.0.0",
        "--status", "challenger", "--assigned-by", "bob", "--reactivate",
    ])
    assert ok.exit_code == 0
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_cli_promote.py -v`
Expected: FAIL — no `promote` subcommand.

- [ ] **Step 3: Add `promote` subcommand**

Append to `/mnt/d/Code/ci_reg_poc/src/registry_sdk/cli.py`:

```python
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
@click.option("--reactivate", is_flag=True,
              help="Required when un-retiring a version (safety guard).")
@click.pass_context
def promote(ctx, family, version, status, assigned_by, note, reactivate):
    """Append a status event for (family, version)."""
    from registry_sdk.store.base import Status
    store = ctx.obj["store"]
    try:
        store.promote(
            family, version, Status(status),
            assigned_by=assigned_by, note=note, reactivate=reactivate,
        )
    except (ValueError, KeyError) as e:
        raise click.ClickException(str(e))
    click.echo(f"Promoted: {family}@{version} -> {status}")
```

- [ ] **Step 4: Run tests to verify green**

Run: `pytest tests/test_cli_promote.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/registry_sdk/cli.py tests/test_cli_promote.py
git commit -m "feat(cli): registry-sdk promote with reactivate guard"
```

---

## Task 12: Query planner (single- and multi-version)

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/planner.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_planner.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_planner.py`:

```python
"""Tests for the query planner.

The planner takes one or more ModelSpecs plus a list of requested canonical
metric names, and returns a Spark DataFrame with columns
[measurement_key, ..metric_columns.., version?] where metrics have been
resolved through aliases and joined across tables.
"""
import pytest

from registry_sdk import Metric, ModelSpec, Table
from registry_sdk.planner import plan_for_spec, plan_for_specs


# --- fixtures for fake result tables ----------------------------------------

def _seed_result_tables(spark, db):
    """Create fake result tables in the local warehouse for planner tests."""
    # v3 shared: canonical column 'ate'
    spark.createDataFrame(
        [("e1", 0.10, 0.05, 0.15), ("e2", 0.20, 0.10, 0.30)],
        schema="experiment_id STRING, ate DOUBLE, ci_lo DOUBLE, ci_hi DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v3")
    # v3 heterogeneity
    spark.createDataFrame(
        [("e1", 0.4), ("e2", 0.9)],
        schema="experiment_id STRING, het_score DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.het_v3")
    # v2 shared: physical column 'te' (former canonical name)
    spark.createDataFrame(
        [("e1", 0.09), ("e2", 0.19)],
        schema="experiment_id STRING, te DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v2")


def _spec_v3(db):
    return ModelSpec(
        family="uplift", version="3.0.0", measurement_key="experiment_id",
        tables=[
            Table(name="shared", path=f"{db}.shared_v3", key="experiment_id", metrics=[
                Metric(name="treatment_effect", column="ate",
                       dtype="double", aliases=["te"]),
                Metric(name="ci_lower", column="ci_lo", dtype="double"),
                Metric(name="ci_upper", column="ci_hi", dtype="double"),
            ]),
            Table(name="het", path=f"{db}.het_v3", key="experiment_id", metrics=[
                Metric(name="cate_variance", column="het_score", dtype="double"),
            ]),
        ],
    )


def _spec_v2(db):
    return ModelSpec(
        family="uplift", version="2.9.0", measurement_key="experiment_id",
        tables=[
            Table(name="shared", path=f"{db}.shared_v2", key="experiment_id", metrics=[
                # v2 used 'te' as the canonical name; in v3 we renamed to
                # 'treatment_effect'. Alias enables cross-version continuity.
                Metric(name="treatment_effect", column="te",
                       dtype="double", aliases=["te"]),
            ]),
        ],
    )


# --- single-version tests ---------------------------------------------------

def test_single_version_join_across_tables(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    spec = _spec_v3(registry_db)
    df = plan_for_spec(spark, spec, metrics=["treatment_effect", "cate_variance"])
    rows = {r["experiment_id"]: r for r in df.collect()}
    assert set(rows) == {"e1", "e2"}
    assert rows["e1"]["treatment_effect"] == 0.10
    assert rows["e1"]["cate_variance"] == 0.4


def test_single_version_resolves_alias_to_physical_column(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    spec = _spec_v3(registry_db)
    df = plan_for_spec(spark, spec, metrics=["te"])  # alias
    assert "treatment_effect" in df.columns
    assert "te" not in df.columns
    assert df.filter("experiment_id = 'e1'").first()["treatment_effect"] == 0.10


def test_unknown_metric_raises(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    spec = _spec_v3(registry_db)
    with pytest.raises(KeyError, match="unknown metric"):
        plan_for_spec(spark, spec, metrics=["nope"])


# --- multi-version tests ----------------------------------------------------

def test_multi_version_union_aligns_alias_across_versions(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    v2, v3 = _spec_v2(registry_db), _spec_v3(registry_db)
    df = plan_for_specs(spark, [v2, v3], metrics=["treatment_effect"])
    assert set(df.columns) == {"experiment_id", "treatment_effect", "version"}
    rows = sorted(
        (r["version"], r["experiment_id"], r["treatment_effect"])
        for r in df.collect()
    )
    assert rows == [
        ("2.9.0", "e1", 0.09), ("2.9.0", "e2", 0.19),
        ("3.0.0", "e1", 0.10), ("3.0.0", "e2", 0.20),
    ]


def test_multi_version_pads_missing_metrics_with_null(spark, registry_db):
    """v2 has no cate_variance; requesting it must union with NULLs, not fail."""
    _seed_result_tables(spark, registry_db)
    v2, v3 = _spec_v2(registry_db), _spec_v3(registry_db)
    df = plan_for_specs(spark, [v2, v3], metrics=["treatment_effect", "cate_variance"])
    assert set(df.columns) == {
        "experiment_id", "treatment_effect", "cate_variance", "version",
    }
    v2_rows = df.filter("version = '2.9.0'").collect()
    assert all(r["cate_variance"] is None for r in v2_rows)
    v3_rows = df.filter("version = '3.0.0' AND experiment_id = 'e1'").collect()
    assert v3_rows[0]["cate_variance"] == 0.4
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_planner.py -v`
Expected: `ImportError`.

- [ ] **Step 3: Implement `planner.py`**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/planner.py`:

```python
"""Plan and execute cross-version metric queries."""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Iterable

from pyspark.sql import functions as F

from registry_sdk.spec import ModelSpec, Table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def plan_for_spec(
    spark: "SparkSession", spec: ModelSpec, *, metrics: Iterable[str],
) -> "DataFrame":
    """Resolve requested metric names against one spec and return a joined DataFrame.

    Group requested names by their owning table, select physical columns aliased
    to their canonical names, then outer-join across tables on the measurement key.
    """
    by_table: dict[Table, list[tuple[str, str]]] = defaultdict(list)
    for name in metrics:
        tbl, canonical, column = spec.resolve_metric(name)
        by_table[tbl].append((canonical, column))

    per_table: list["DataFrame"] = []
    for tbl, cols in by_table.items():
        select = [F.col(tbl.key).alias(spec.measurement_key)]
        select += [F.col(col).alias(canonical) for canonical, col in cols]
        per_table.append(spark.table(tbl.path).select(*select))

    joined = per_table[0]
    for df in per_table[1:]:
        joined = joined.join(df, on=spec.measurement_key, how="outer")
    return joined


def plan_for_specs(
    spark: "SparkSession", specs: Iterable[ModelSpec], *, metrics: Iterable[str],
) -> "DataFrame":
    """Plan per spec, tag with `version`, union with NULL padding for missing metrics."""
    wanted = list(metrics)
    parts: list["DataFrame"] = []
    for spec in specs:
        present = []
        for m in wanted:
            try:
                spec.resolve_metric(m)
                present.append(m)
            except KeyError:
                continue
        if present:
            part = plan_for_spec(spark, spec, metrics=present)
        else:
            part = spark.createDataFrame([], schema=f"{spec.measurement_key} STRING")
        for m in wanted:
            if m not in part.columns:
                part = part.withColumn(m, F.lit(None).cast("double"))
        part = part.withColumn("version", F.lit(spec.version))
        parts.append(part.select(spec.measurement_key, *wanted, "version"))

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p, allowMissingColumns=True)
    return out
```

- [ ] **Step 4: Run tests to verify green**

Run: `pytest tests/test_planner.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/registry_sdk/planner.py tests/test_planner.py
git commit -m "feat(planner): single/multi-version query planning with alias resolution"
```

---

## Task 13: RegistryClient + `get_results` consumer API

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/client.py`
- Modify: `/mnt/d/Code/ci_reg_poc/src/registry_sdk/__init__.py`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_client.py`

- [ ] **Step 1: Write failing tests**

Create `/mnt/d/Code/ci_reg_poc/tests/test_client.py`:

```python
"""Tests for RegistryClient (consumer-facing API)."""
import pytest

from registry_sdk import Metric, ModelSpec, RegistryClient, Table
from registry_sdk.store.base import Status
from registry_sdk.store.spark_hive import SparkHiveSpecStore


def _seed_result_tables(spark, db):
    spark.createDataFrame(
        [("e1", 0.10), ("e2", 0.20)],
        schema="experiment_id STRING, ate DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v3")
    spark.createDataFrame(
        [("e1", 0.09), ("e2", 0.19)],
        schema="experiment_id STRING, te DOUBLE",
    ).write.format("delta").saveAsTable(f"{db}.shared_v2")


def _spec(family, version, db, table_suffix, phys):
    return ModelSpec(
        family=family, version=version, measurement_key="experiment_id",
        tables=[Table(
            name="shared", path=f"{db}.shared_{table_suffix}",
            key="experiment_id", metrics=[
                Metric(name="treatment_effect", column=phys,
                       dtype="double", aliases=["te"]),
            ],
        )],
    )


def _seed_registry(spark, db):
    store = SparkHiveSpecStore(spark=spark, database=db)
    store.ensure_tables()
    for spec in [
        _spec("uplift", "2.9.0", db, "v2", "te"),
        _spec("uplift", "3.0.0", db, "v3", "ate"),
    ]:
        store.put(spec, git_repo="org/uplift-model", git_tag=f"v{spec.version}",
                  git_sha="a" * 40, registered_by="alice", sdk_version="0.1.0")
    return store


def test_get_results_by_explicit_version(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    store = _seed_registry(spark, registry_db)
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(family="uplift", version="3.0.0",
                            metrics=["treatment_effect"])
    assert {"experiment_id", "treatment_effect"} <= set(df.columns)
    rows = {r["experiment_id"]: r["treatment_effect"] for r in df.collect()}
    assert rows == {"e1": 0.10, "e2": 0.20}


def test_get_results_by_status_production_returns_one_version(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    store = _seed_registry(spark, registry_db)
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(family="uplift", status="production",
                            metrics=["treatment_effect"])
    rows = df.collect()
    assert all(r["version"] == "3.0.0" for r in rows)


def test_get_results_multi_status_unions(spark, registry_db):
    _seed_result_tables(spark, registry_db)
    store = _seed_registry(spark, registry_db)
    store.promote("uplift", "3.0.0", Status.PRODUCTION, assigned_by="bob")
    store.promote("uplift", "2.9.0", Status.CHALLENGER, assigned_by="bob")
    client = RegistryClient(store=store, spark=spark)
    df = client.get_results(
        family="uplift", status=["production", "challenger"],
        metrics=["treatment_effect"],
    )
    versions = {r["version"] for r in df.collect()}
    assert versions == {"3.0.0", "2.9.0"}


def test_get_results_rejects_status_and_version_together(spark, registry_db):
    store = _seed_registry(spark, registry_db)
    client = RegistryClient(store=store, spark=spark)
    with pytest.raises(ValueError, match="either"):
        client.get_results(family="uplift", version="3.0.0",
                           status="production", metrics=["treatment_effect"])


def test_discovery_wrappers(spark, registry_db):
    store = _seed_registry(spark, registry_db)
    client = RegistryClient(store=store, spark=spark)
    assert client.list_families() == ["uplift"]
    assert sorted(client.list_versions("uplift")) == ["2.9.0", "3.0.0"]
    described = client.describe("uplift", "3.0.0")
    assert described.spec.version == "3.0.0"
    hist = client.history("uplift", "3.0.0")
    assert len(hist) == 1 and hist[0].status == Status.EXPERIMENT
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `pytest tests/test_client.py -v`
Expected: `ImportError` — `RegistryClient` not exported.

- [ ] **Step 3: Implement `client.py`**

Create `/mnt/d/Code/ci_reg_poc/src/registry_sdk/client.py`:

```python
"""Consumer-facing entry point: RegistryClient."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterable

from registry_sdk.planner import plan_for_spec, plan_for_specs
from registry_sdk.store.base import Registration, SpecStore, Status, StatusEvent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class RegistryClient:
    store: SpecStore
    spark: "SparkSession"

    # ---- discovery ----------------------------------------------------------

    def list_families(self) -> list[str]:
        return self.store.list_families()

    def list_versions(self, family: str) -> list[str]:
        return self.store.list_versions(family)

    def describe(self, family: str, version: str) -> Registration:
        return self.store.get(family, version)

    def history(self, family: str, version: str) -> list[StatusEvent]:
        return self.store.history(family, version)

    # ---- query --------------------------------------------------------------

    def get_results(
        self,
        *,
        family: str,
        metrics: Iterable[str],
        version: str | None = None,
        status: str | list[str] | None = None,
        as_of: datetime | str | None = None,
    ) -> "DataFrame":
        if version is not None and status is not None:
            raise ValueError("pass either `version` or `status`, not both")
        if version is None and status is None:
            raise ValueError("one of `version` or `status` is required")

        if as_of is not None and isinstance(as_of, str):
            as_of = datetime.fromisoformat(as_of)

        if version is not None:
            regs = [self.store.get(family, version)]
        else:
            statuses = (
                [Status(status)] if isinstance(status, str)
                else [Status(s) for s in status]
            )
            regs = self.store.by_status(family, statuses, as_of=as_of)
            if not regs:
                raise LookupError(
                    f"no {family!r} versions match status(es) {status!r}"
                )

        specs = [r.spec for r in regs]
        if len(specs) == 1 and version is not None:
            # Single explicit version: don't attach a `version` column, keep
            # the surface minimal.
            return plan_for_spec(self.spark, specs[0], metrics=metrics)
        return plan_for_specs(self.spark, specs, metrics=metrics)
```

- [ ] **Step 4: Export `RegistryClient` from the package**

Replace `/mnt/d/Code/ci_reg_poc/src/registry_sdk/__init__.py`:

```python
"""registry_sdk — model registry SDK."""

from registry_sdk.client import RegistryClient
from registry_sdk.spec import Metric, ModelSpec, Table

__version__ = "0.1.0"
__all__ = ["Metric", "ModelSpec", "Table", "RegistryClient", "__version__"]
```

- [ ] **Step 5: Run tests to verify green**

Run: `pytest tests/test_client.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `pytest -x -q`
Expected: All previous tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/registry_sdk/client.py src/registry_sdk/__init__.py tests/test_client.py
git commit -m "feat(client): RegistryClient.get_results + discovery wrappers"
```

---

## Task 14: Example model repo + mock GHA workflow

**Files:**
- Create: `/mnt/d/Code/ci_reg_poc/examples/uplift-model/model_spec.py`
- Create: `/mnt/d/Code/ci_reg_poc/examples/uplift-model/.github/workflows/register.yml`
- Create: `/mnt/d/Code/ci_reg_poc/examples/uplift-model/README.md`
- Create: `/mnt/d/Code/ci_reg_poc/tests/test_example_spec.py`

- [ ] **Step 1: Write failing test that loads the example spec**

Create `/mnt/d/Code/ci_reg_poc/tests/test_example_spec.py`:

```python
"""Prove the example model_spec.py loads via the same importer the CLI uses."""
from pathlib import Path

from registry_sdk import ModelSpec
from registry_sdk.cli import _load_spec


def test_example_uplift_spec_loads():
    path = Path(__file__).parent.parent / "examples" / "uplift-model" / "model_spec.py"
    spec = _load_spec(path)
    assert isinstance(spec, ModelSpec)
    assert spec.family == "uplift"
    # Alias round-trip check
    tbl, col = spec.resolve_metric("te")
    assert col == "ate"
```

- [ ] **Step 2: Run test to confirm failure**

Run: `pytest tests/test_example_spec.py -v`
Expected: FAIL — file doesn't exist.

- [ ] **Step 3: Write `examples/uplift-model/model_spec.py`**

```python
"""Example model_spec.py — imported by the register CLI at tag time."""
from registry_sdk import Metric, ModelSpec, Table

spec = ModelSpec(
    family="uplift",
    version="3.1.0",
    measurement_key="experiment_id",
    tables=[
        Table(
            name="shared",
            path="analytics.uplift.shared_v3",
            key="experiment_id",
            metrics=[
                Metric(name="treatment_effect", column="ate",
                       dtype="double", aliases=["te"]),
                Metric(name="ci_lower", column="ci_lo", dtype="double"),
                Metric(name="ci_upper", column="ci_hi", dtype="double"),
            ],
        ),
        Table(
            name="heterogeneity",
            path="analytics.uplift.het_v3",
            key="experiment_id",
            metrics=[
                Metric(name="cate_variance", column="het_score",
                       dtype="double", aliases=["heterogeneity_score"]),
            ],
        ),
    ],
)
```

- [ ] **Step 4: Write GHA workflow**

Create `/mnt/d/Code/ci_reg_poc/examples/uplift-model/.github/workflows/register.yml`:

```yaml
# Register the ModelSpec whenever a v*.*.* tag is pushed. This runs the same
# `registry-sdk register` command a developer can run locally.
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
        run: pip install registry-sdk
      - name: Type-check the spec
        run: |
          pip install mypy
          mypy model_spec.py --strict
      # In a real setup this step would authenticate to Databricks and export
      # REGISTRY_STORE_CONFIG pointing at the UC schema for the registry.
      - name: Register
        env:
          REGISTRY_STORE_CONFIG: ${{ vars.REGISTRY_STORE_CONFIG }}
        run: registry-sdk register --spec-path model_spec.py
```

- [ ] **Step 5: Write example README**

Create `/mnt/d/Code/ci_reg_poc/examples/uplift-model/README.md`:

```markdown
# Example: uplift-model

Minimal example of a downstream repo that registers a `ModelSpec` on tag push.

Local dry-run:

    registry-sdk register \
        --spec-path model_spec.py \
        --git-repo local/uplift-model \
        --git-tag v3.1.0 \
        --git-sha $(git rev-parse HEAD) \
        --registered-by "$USER"
```

- [ ] **Step 6: Run tests to verify green + full suite**

Run: `pytest -x -q`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add examples/uplift-model tests/test_example_spec.py
git commit -m "docs(example): add uplift-model example spec + GHA workflow"
```

---

## Task 15: Final sweep — full suite + README update

**Files:**
- Modify: `/mnt/d/Code/ci_reg_poc/README.md`

- [ ] **Step 1: Run the full test suite once more**

Run: `pytest -q`
Expected: All tests pass, no warnings from our code (Spark deprecation warnings are filtered in `pyproject.toml`).

- [ ] **Step 2: Manual walkthrough via the CLI**

```bash
export REGISTRY_STORE_CONFIG='{"backend":"spark_hive","database":"walkthrough_registry"}'
export REGISTRY_WAREHOUSE_DIR=/tmp/registry_sdk_walkthrough/warehouse
export REGISTRY_METASTORE_DIR=/tmp/registry_sdk_walkthrough/metastore_db
rm -rf /tmp/registry_sdk_walkthrough
cd examples/uplift-model
registry-sdk register \
    --spec-path model_spec.py \
    --git-repo local/uplift-model \
    --git-tag v3.1.0 \
    --git-sha $(python -c "import secrets;print(secrets.token_hex(20))") \
    --registered-by "$USER"
registry-sdk promote --family uplift --version 3.1.0 \
    --status challenger --assigned-by "$USER"
registry-sdk promote --family uplift --version 3.1.0 \
    --status production --assigned-by "$USER" --note "smoke test"
```

Expected: three commands succeed; each prints a confirmation line.

- [ ] **Step 3: Update the top-level README with the walkthrough**

Append to `/mnt/d/Code/ci_reg_poc/README.md`:

```markdown

## Walkthrough

    export REGISTRY_STORE_CONFIG='{"backend":"spark_hive","database":"registry"}'
    cd examples/uplift-model
    registry-sdk register --spec-path model_spec.py \
        --git-repo local/uplift-model --git-tag v3.1.0 \
        --git-sha $(git rev-parse HEAD) --registered-by "$USER"
    registry-sdk promote --family uplift --version 3.1.0 \
        --status production --assigned-by "$USER"

Then from a notebook / Python REPL:

    from registry_sdk import RegistryClient
    from registry_sdk.spark_session import build_local_spark_session
    from registry_sdk.store import get_store

    spark = build_local_spark_session(
        warehouse_dir="~/.registry_sdk/warehouse",
        metastore_dir="~/.registry_sdk/metastore_db",
    )
    client = RegistryClient(store=get_store(spark), spark=spark)
    client.get_results(
        family="uplift", status="production",
        metrics=["treatment_effect", "cate_variance"],
    ).show()
```

- [ ] **Step 4: Final commit**

```bash
git add README.md
git commit -m "docs: add end-to-end walkthrough"
```

---

## Self-review checklist (executed while writing this plan)

**Spec coverage:** Every item in "Suggested POC Scope" maps to at least one task —
1. Pydantic models → Task 2
2. `SpecStore` interface + local implementation → Tasks 4–8
3. Registration CLI (spec loading, tag matching, UC validation missing-vs-drift, idempotency) → Tasks 9, 10
4. Promotion API with atomic production swap → Task 8 (store) + Task 11 (CLI)
5. Query planner single/multi-version with alias resolution → Task 12
6. `get_results` supporting `version=`, `status=`, `as_of=` → Task 13
7. Discovery API (`list_families`, `list_versions`, `describe`, `history`) → Task 7 (store) + Task 13 (client wrappers)
8. Example model repo + mock GHA → Task 14

**Design-doc rules covered by tests:**
- Default status on registration = experiment → `test_put_inserts_registration_and_initial_experiment_event`
- One-production-per-family via atomic swap → `test_promote_to_production_retires_current_prod_atomically`
- Un-retire needs `--reactivate` → `test_unretire_requires_reactivate_flag` and its CLI twin
- Missing table warns, wrong schema errors → `test_missing_table_produces_warning_not_error`, `test_missing_column_produces_error`, `test_dtype_mismatch_produces_error`
- Cross-version alias continuity → `test_multi_version_union_aligns_alias_across_versions`
- Cross-table alias-collision rejection (loud) → `test_resolve_metric_raises_on_cross_table_alias_collision`
- Point-in-time status → `test_current_status_respects_as_of`

**Naming consistency:** `Status` enum values, method signatures (`by_status`, `current_status`, `promote(family, version, status, *, assigned_by, note, reactivate)`), and `RegistryClient.get_results` parameter shape stay identical across tasks 4/7/8/11/13.

**No placeholders:** every code block is complete; test names describe behaviour; commands include expected output.
