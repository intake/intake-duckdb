import numpy as np
from intake.source import base

from . import __version__


class DuckDBSource(base.DataSource):
    """
    One-shot SQL to dataframe reader (no partitioning)

    Caches entire dataframe in memory.

    Parameters
    ----------
    uri: str or None
        Full connection string in sqlalchemy syntax
    sql_expr: str
        Query expression to pass to the DB backend
    sql_kwargs: dict
        Further arguments to pass to pandas.read_sql
    """

    name = "duckdb"
    version = __version__
    container = "dataframe"
    partition_access = True

    def __init__(
        self,
        uri=None,
        sql_expr=None,
        connection=None,
        table=None,
        chunks=None,
        duckdb_kwargs={},
        metadata={},
    ):
        self._init_args = {
            "uri": uri,
            "sql_expr": sql_expr,
            "table": table,
            "chunks": chunks,
            "duckdb_kwargs": duckdb_kwargs,
            "metadata": metadata,
        }

        if sql_expr is None and table is None:
            err = "One of 'sql_expr' or 'table' is required"
            raise ValueError(err)

        if sql_expr is not None and table is not None:
            err = "Only one of 'sql_expr' or 'table' is allowed"
            raise ValueError(err)

        if uri is None and connection is None:
            err = "One of 'uri' or 'connection' is required"
            raise ValueError(err)

        self._uri = uri
        self._con = connection
        self._sql_expr = sql_expr or f"SELECT * FROM {table}"
        self._duckdb_kwargs = duckdb_kwargs
        self._chunks = chunks or 1
        self._schema = None
        self._dataframe = None
        self._con = None
        self._bins = None

        super(DuckDBSource, self).__init__(metadata=metadata)

    def _load(self):
        self._load_metadata()
        self._dataframe = self._duckdb.df()

    def _get_schema(self):
        if self._schema is None:
            import duckdb

            self._con = self._con or duckdb.connect(self._uri)
            self._duckdb = self._con.sql(self._sql_expr)
            self._bins = np.linspace(
                0, self._duckdb.shape[0], self._chunks + 1, dtype=int
            )

            shape = self._duckdb.shape
            columns = self._duckdb.columns
            dtypes = self._duckdb.types

            self._schema = base.Schema(
                datashape=None,
                dtype=dict(zip(columns, dtypes)),
                shape=shape,
                npartitions=self._chunks,
                extra_metadata={},
            )

        return self._schema

    def _get_partition(self, i):
        self._load_metadata()

        start = self._bins[i]
        stop = self._bins[i + 1]

        if self._dataframe is not None:
            return self._dataframe[start:stop]

        chunked_sql_expr = f"""
            {self._sql_expr}
            WHERE rowid >= {start}
            AND rowid < {stop}
        """

        return self._con.sql(chunked_sql_expr).df()

    def read(self):
        self._load()
        return self._dataframe

    def _close(self):
        self._dataframe = None
        self._con.close()
