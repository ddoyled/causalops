# Spec Models

Frozen Pydantic models that describe a versioned contract for one model family's result tables. These are the core data structures that producers define and consumers query against.

## Dtype

::: causalops.spec.Dtype

## Metric

::: causalops.spec.Metric
    options:
      members:
        - name
        - column
        - dtype
        - aliases

## Table

::: causalops.spec.Table
    options:
      members:
        - name
        - path
        - key
        - metrics

## ModelSpec

::: causalops.spec.ModelSpec
    options:
      members:
        - family
        - version
        - measurement_key
        - tables
        - resolve_metric
