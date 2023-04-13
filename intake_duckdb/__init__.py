from importlib.metadata import version

__version__ = version("intake-duckdb")
del version

# import intake
from .base import DuckDBSource
from .catalog import DuckDBCatalog
from .transform import DuckDBTransform
