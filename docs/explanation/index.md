# Explanation

Understanding-oriented articles that explain the *why* behind design decisions in causalops.

These are not step-by-step instructions — see [Tutorials](../tutorials/index.md) and [How-To Guides](../how-to/index.md) for that. Instead, these articles give you the reasoning behind the architecture so you can make informed decisions when extending or integrating with the system.

- [Architecture](architecture.md) — module structure, the three-persona model, and why the package is shaped the way it is.
- [The Lifecycle Model](lifecycle-model.md) — why four statuses, atomic production swaps, and append-only event streams.
- [Alias Resolution](alias-resolution.md) — how canonical names and aliases provide cross-version compatibility.
- [Planner Internals](planner-internals.md) — join strategy, multi-version unions, NULL padding, and the dual-path table reader.
