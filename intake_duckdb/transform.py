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
        self.sources = self.get_sources(targets)

    def get_sources(self, targets):
        for targ in targets:
            if isinstance(targ, DataSource):
                yield targ
            else:
                yield self.cat[targ]

    def _get_schema(self):
        context = {source.name: source.read() for source in self.sources}
        return super()._get_schema(context)


def all_targets(targets, cat, kwargs, cat_kwargs):
    from intake.source.derived import get_source

    return [get_source(targ, cat, kwargs.get(targ, {}), cat_kwargs) for targ in targets]
