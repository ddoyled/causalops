# Tutorials

These four tutorials walk you through causalops end-to-end, from defining
your first model spec to querying results downstream. They are designed to
be followed in order — each builds on the state created by the previous one.

| # | Tutorial | Persona |
|---|----------|---------|
| 1 | [Define a ModelSpec](define-a-model-spec.md) | Model producer |
| 2 | [Register & Validate](register-and-validate.md) | Model producer |
| 3 | [Promote through the Lifecycle](promote-lifecycle.md) | Platform team |
| 4 | [Query Results](query-results.md) | Consumer / downstream analytics |

!!! tip "Before you start"
    Install causalops and its dependencies:

    ```bash
    pip install -e '.[dev]'
    ```

    You will also need Java 17 for the local Spark session:

    ```bash
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # or your Java 17 path
    ```
