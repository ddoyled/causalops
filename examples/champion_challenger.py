"""champion_challenger.py — compare two explicit versions side-by-side.

Assumes ``python scripts/seed_examples.py`` has already populated the
local warehouse and registry. This script is a pure consumer.

Demonstrates the champion/challenger comparison workflow:

1. Hard-code the two version strings to compare (champion = current
   production, challenger = candidate under evaluation).
2. Pull both versions' results in one ``get_results`` call using the
   ``versions`` parameter.
3. Pivot into a side-by-side table and compute deltas to evaluate
   whether the challenger outperforms the champion.

Run from the repo root:

    python examples/champion_challenger.py
"""

from __future__ import annotations

from pyspark.sql import functions as F

from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

FAMILY = "uplift"
CHAMPION_VERSION = "1.0.0"
CHALLENGER_VERSION = "1.1.0"

METRICS = ["treatment_effect", "ci_lower", "ci_upper"]


def main() -> None:
    spark = build_local_spark_session()
    client = RegistryClient(store=get_store(), spark=spark)

    # --- 1. Pull champion & challenger results in one call -------------------
    df = client.get_results(
        family=FAMILY,
        versions=[CHAMPION_VERSION, CHALLENGER_VERSION],
        metrics=METRICS,
    )

    print(f"Champion (v{CHAMPION_VERSION}) vs Challenger (v{CHALLENGER_VERSION})")
    print(f"Raw union ({df.count()} rows):")
    df.show(10, truncate=False)

    # --- 2. Pivot into side-by-side columns ----------------------------------
    comparison = (
        df.groupBy("experiment_id")
        .pivot("version")
        .agg(
            F.first("treatment_effect").alias("treatment_effect"),
            F.first("ci_lower").alias("ci_lower"),
            F.first("ci_upper").alias("ci_upper"),
        )
    )

    print("Side-by-side comparison:")
    comparison.show(10, truncate=False)

    # --- 3. Compute deltas ---------------------------------------------------
    champ = CHAMPION_VERSION
    chall = CHALLENGER_VERSION

    delta = comparison.withColumn(
        "te_delta",
        F.col(f"`{chall}_treatment_effect`") - F.col(f"`{champ}_treatment_effect`"),
    ).select(
        "experiment_id",
        F.col(f"`{champ}_treatment_effect`").alias("champion_te"),
        F.col(f"`{chall}_treatment_effect`").alias("challenger_te"),
        "te_delta",
    )

    print("Treatment-effect deltas (challenger - champion):")
    delta.show(10, truncate=False)

    # --- 4. Summary statistics -----------------------------------------------
    stats = delta.agg(
        F.mean("te_delta").alias("mean_delta"),
        F.stddev("te_delta").alias("stddev_delta"),
        F.sum(F.when(F.col("te_delta") > 0, 1).otherwise(0)).alias("challenger_wins"),
        F.count("te_delta").alias("n_experiments"),
    )
    print("Summary:")
    stats.show(truncate=False)


if __name__ == "__main__":
    main()
