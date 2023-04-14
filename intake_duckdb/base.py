import numpy as np
from intake.source import DataSource
from intake.source.base import Schema

from . import __version__


class DuckDBSource(DataSource):
    """
    DuckDB table to dataframe reader. Can take either a table name or a SQL expression.
    Partitionable.

    Caches entire dataframe in memory.

    Parameters
    ----------
    uri: str or None
        Path to local duckdb file
    sql_expr: str or None
        Query expression to pass to the DB backend
    connection: duckdb.DuckDBPyConnection or None
        Existing connection to DB backend
    table: str or None
        Table name
    chunks: int or None
        Number of partitions, default is 1
    metadata: dict
        Additional metadata to pass to parent class
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
        metadata={},
    ):
        self._init_args = {
            "uri": uri,
            "sql_expr": sql_expr,
            "table": table,
            "chunks": chunks,
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
        self._chunks = chunks or 1
        self._schema = None
        self._dataframe = None
        self._con = None
        self._bins = None

        super(DuckDBSource, self).__init__(metadata=metadata)

    def _load(self):
        self._load_metadata()
        self._dataframe = self._duckdb.df()

    def _get_schema(self, context=None):
        if self._schema is None:
            import duckdb

            if context is not None:
                globals().update(context)

            self._con = self._con or duckdb.connect(self._uri)
            self._duckdb = self._con.sql(self._sql_expr)
            self._bins = np.linspace(
                0, self._duckdb.shape[0], self._chunks + 1, dtype=int
            )

            shape = self._duckdb.shape
            columns = self._duckdb.columns
            dtypes = self._duckdb.types

            self._schema = Schema(
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
