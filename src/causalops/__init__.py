"""causalops — registry for causal inference model results."""

from causalops.client import RegistryClient
from causalops.spec import Metric, ModelSpec, Table

__version__ = "0.1.0"
__all__ = ["Metric", "ModelSpec", "Table", "RegistryClient", "__version__"]
