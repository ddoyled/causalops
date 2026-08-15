"""Prove the example model_spec.py loads via the same importer the CLI uses."""
from pathlib import Path

from causalops import ModelSpec
from causalops.cli import _load_spec


def test_example_uplift_spec_loads():
    path = Path(__file__).parent.parent / "examples" / "uplift-model" / "model_spec.py"
    spec = _load_spec(path)
    assert isinstance(spec, ModelSpec)
    assert spec.family == "uplift"
    # Alias round-trip check
    tbl, canonical, col = spec.resolve_metric("te")
    assert col == "ate"
