from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

# import intake
from .base import DuckDBSource

# from .intake_duckdb import (SQLSource, SQLSourceAutoPartition, SQLSourceManualPartition)
# from .duckdb_cat import DuckDBCatalog
