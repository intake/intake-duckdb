from importlib.metadata import version

__version__ = version("intake-duckdb")
del version

# import intake
from .base import DuckDBSource

# from .intake_duckdb import (SQLSource, SQLSourceAutoPartition, SQLSourceManualPartition)
# from .duckdb_cat import DuckDBCatalog
