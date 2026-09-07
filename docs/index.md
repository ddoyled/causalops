# causalops

**Registry for causal inference model results on Databricks.**

Model teams define versioned contracts ([ModelSpecs](reference/spec.md)) that declare their result tables and metrics, register them via CLI or CI, and promote them through a lifecycle (experiment → challenger → production → retired). Consumers query results by version or status through a unified API that resolves canonical metric names, joins across tables, and unions across versions automatically.

## Install

```bash
pip install causalops
```

## Quick start

```python
from causalops import RegistryClient
from causalops.store import get_store
from causalops.utils import build_local_spark_session

spark = build_local_spark_session()
client = RegistryClient(store=get_store(), spark=spark)

# Query production results
df = client.get_results(
    family="uplift",
    status="production",
    metrics=["treatment_effect", "ci_lower", "ci_upper"],
)
df.show()
```

## Documentation

This documentation follows the [Diataxis](https://diataxis.fr/) framework:

| Section | Purpose | Start here if you want to... |
|---|---|---|
| [Tutorials](tutorials/index.md) | Learning-oriented step-by-step guides | ...learn how causalops works from scratch |
| [How-To Guides](how-to/index.md) | Task-oriented recipes for specific goals | ...accomplish a specific task |
| [Reference](reference/index.md) | Auto-generated API documentation | ...look up a class, function, or CLI flag |
| [Explanation](explanation/index.md) | Understanding-oriented design discussions | ...understand *why* things work this way |

## By role

**Model producer** — you own a causal model and want to register its results.

- [Define a ModelSpec](tutorials/define-a-model-spec.md) — declare your tables and metrics
- [Register & Validate](tutorials/register-and-validate.md) — validate schemas and register via CLI
- [CI Registration with GitHub Actions](how-to/ci-registration.md) — automate registration on tag push

**Platform team** — you manage the model lifecycle across teams.

- [Promote through Lifecycle](tutorials/promote-lifecycle.md) — experiment → challenger → production → retired
- [Custom Store Backend](how-to/custom-store-backend.md) — implement the SpecStore ABC for your infrastructure
- [Databricks Migration](how-to/databricks-migration.md) — lift-and-shift from local dev to Databricks

**Consumer** — you query model results from downstream processes.

- [Query Results](tutorials/query-results.md) — query by version, status, or point in time
- [Production-of-Record Collection](how-to/production-of-record.md) — build a unified production table across families
- [Multi-Version Comparison](how-to/multi-version-comparison.md) — compare production and challenger side-by-side
