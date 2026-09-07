# API Reference

The `causalops` package is organized around three personas:

| Persona | Primary modules | Typical tasks |
|---|---|---|
| **Producer** (model team) | [Spec Models](spec.md), [Validation](validation.md), [CLI](cli.md) | Define specs, validate schemas, register versions |
| **Consumer** (downstream analytics) | [RegistryClient](client.md), [Planner](planner.md) | Query results, discover versions, build production-of-record tables |
| **Platform team** | [Store](store.md), [CLI](cli.md) | Promote versions, implement storage backends |

Supporting modules:

- [Utilities](utils.md) — Spark session factory and table reader
- [Paths](paths.md) — Default filesystem locations for local dev
