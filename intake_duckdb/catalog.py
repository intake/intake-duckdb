from collections.abc import Mapping

import duckdb
from intake.catalog.base import Catalog
from intake.catalog.local import LocalCatalogEntry

from . import __version__
from .base import DuckDBSource


class DuckDBCatalog(Catalog):
    """
    Makes data sources out of known tables in the given SQL service

    This uses SQLAlchemy to infer the tables and views on the target server.
    Of these, those which have at least one primary key column will become
    ``SQLSourceAutoPartition`` entries in this catalog.
    """

    name = "duckdb_cat"
    version = __version__

    def __init__(self, uri, views=False, duckdb_kwargs=None, **kwargs):
        self._duckdb_kwargs = duckdb_kwargs or {}
        self._uri = uri

        # TODO: does duckdb have views?
        # self._views = views
        super(DuckDBCatalog, self).__init__(**kwargs)

    def _load(self):
        self._entries = DuckDBEntries(self._uri, self._duckdb_kwargs)


class DuckDBEntries(Mapping):
    def __init__(self, uri, duckdb_kwargs):
        self._uri = uri
        self._duckdb_kwargs = duckdb_kwargs
        self._tables = None
        self._cache = {}

    def tables(self):
        if self._tables is None:
            with duckdb.connect(self._uri, read_only=True) as con:
                self._tables = [
                    table[0] for table in con.execute("SHOW TABLES").fetchall()
                ]

        return self._tables

    def entry(self, name):
        if name in self._cache:
            return self._cache[name]

        description = f"DuckDB table {name} from {self._uri}"

        args = {
            "uri": self._uri,
            "table": name,
            "duckdb_kwargs": self._duckdb_kwargs,
        }

        e = LocalCatalogEntry(
            name=name,
            description=description,
            driver="duckdb",
            args=args,
        )

        e._plugin = [DuckDBSource]
        self._cache[name] = e

        return e

    def keys(self):
        return self.tables()

    def __getitem__(self, item):
        return self.entry(item)

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())
