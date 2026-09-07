# Store

The storage layer for the model registry. Defines the abstract contract (`SpecStore`), the lifecycle status enum, the registration and event data classes, a concrete JSON-file backend, and a factory function.

## Status

::: causalops.store.base.Status

## Registration

::: causalops.store.base.Registration
    options:
      members:
        - spec
        - git_repo
        - git_tag
        - git_sha
        - sdk_version
        - registered_by
        - registered_at
        - family
        - version

## StatusEvent

::: causalops.store.base.StatusEvent
    options:
      members:
        - event_id
        - family
        - version
        - status
        - effective_from
        - assigned_by
        - note

## SpecStore

::: causalops.store.base.SpecStore
    options:
      members:
        - put
        - exists
        - get
        - list_families
        - list_versions
        - promote
        - current_status
        - by_status
        - history

## JsonFileSpecStore

::: causalops.store.json_file.JsonFileSpecStore

## get_store

::: causalops.store.get_store
