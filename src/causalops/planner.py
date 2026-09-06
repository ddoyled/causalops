"""Plan and execute cross-version metric queries."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from causalops.spec import ModelSpec, Table
from causalops.utils import read_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def plan_for_spec(
    spark: SparkSession,
    spec: ModelSpec,
    *,
    metrics: Iterable[str],
    include_columns: Iterable[str] = (),
) -> DataFrame:
    """Resolve requested metric names against one spec and return a joined DataFrame.

    Group requested names by their owning table, select physical columns aliased
    to their canonical names, then outer-join across tables on the measurement key.

    ``include_columns`` are extra physical columns (e.g. ``run_date``,
    ``channel_id``) passed through from the first table that contains them.
    """
    by_table: dict[Table, list[tuple[str, str]]] = defaultdict(list)
    for name in metrics:
        tbl, canonical, column = spec.resolve_metric(name)
        by_table[tbl].append((canonical, column))

    extra = list(include_columns)
    extras_placed = False

    per_table: list[DataFrame] = []
    for tbl, cols in by_table.items():
        select = [F.col(tbl.key).alias(spec.measurement_key)]
        select += [F.col(col).alias(canonical) for canonical, col in cols]
        raw = read_table(spark, tbl.path)
        if extra and not extras_placed:
            available = [c for c in extra if c in raw.columns]
            select += [F.col(c) for c in available]
            extras_placed = True
        per_table.append(raw.select(*select))

    joined = per_table[0]
    for df in per_table[1:]:
        joined = joined.join(df, on=spec.measurement_key, how="outer")
    return joined


def plan_for_specs(
    spark: SparkSession,
    specs: Iterable[ModelSpec],
    *,
    metrics: Iterable[str],
) -> DataFrame:
    """Plan per spec, tag with `version`, union with NULL padding for missing metrics."""
    wanted = list(metrics)
    parts: list[DataFrame] = []
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
