# Validation

Validates a spec's declared tables and columns against the live catalog or Parquet files. Missing tables produce warnings (expected before a pipeline's first run); missing columns or dtype mismatches produce errors.

## ValidationReport

::: causalops.validation.ValidationReport
    options:
      members:
        - errors
        - warnings
        - has_errors
        - has_warnings
        - format
        - format_warnings

## validate_against_uc

::: causalops.validation.validate_against_uc
