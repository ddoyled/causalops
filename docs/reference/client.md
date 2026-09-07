# RegistryClient

The consumer-facing entry point for querying the registry. Bundles a store and a Spark session, then exposes discovery, lifecycle, and query methods.

::: causalops.client.RegistryClient
    options:
      members:
        - store
        - spark
        - list_families
        - list_versions
        - describe
        - history
        - production_windows
        - get_results
