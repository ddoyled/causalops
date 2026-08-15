"""causalops — model registry SDK."""

from causalops.client import RegistryClient
from causalops.spec import Metric, ModelSpec, Table

__version__ = "0.1.0"
__all__ = ["Metric", "ModelSpec", "Table", "RegistryClient", "__version__"]
