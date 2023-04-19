from intake.source import DataSource

from . import __version__
from .base import DuckDBSource


class DuckDBTransform(DuckDBSource):
    """
    Run a DuckDB query on any Intake source for which `.read()` produces a pandas
    DataFrame. Can specify multiple targets within the same catalog. Reads entire
    source of each target into memory.

    Parameters
    ----------
    sql_expr: str
        Query expression to pass to the DB backend
    targets: list[intake.DataSource] or list[str]
        List of Intake data sources or named sources within catalog
    chunks: int or None
        Number of partitions, default is 1
    metadata: dict
        Additional metadata to pass to parent class

    """

    name = "duckdb_transform"
    version = __version__
    container = "dataframe"
    partition_access = True

    def __init__(
        self,
        targets,
        sql_expr,
        chunks=None,
        metadata={},
    ):
        self._base_args = {
            "uri": ":memory:",
            "sql_expr": sql_expr,
            "chunks": chunks,
            "metadata": metadata,
        }

        super().__init__(**self._base_args)
        self.targets = targets
        self.sources = {}

    def get_sources(self):
        for targ in self.targets:
            if isinstance(targ, DataSource):
                self.sources[targ.name] = targ
            else:
                self.sources[targ] = self.cat[targ]

    def _get_schema(self):
        if not self.sources:
            self.get_sources()

        context = {name: source.read() for name, source in self.sources.items()}
        return super()._get_schema(context)
