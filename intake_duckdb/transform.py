from intake.source import DataSource

from . import __version__
from .base import DuckDBSource


class DuckDBTransform(DuckDBSource):
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
