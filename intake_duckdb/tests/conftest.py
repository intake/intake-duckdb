import os

import duckdb
import numpy as np
import pandas as pd
import pytest

from .. import DuckDBSource

TEMP_TABLE = "temp"
HERE = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(scope="module")
def dataframe():
    df = pd.DataFrame(
        {
            "a": np.random.rand(100).tolist(),
            "b": np.random.randint(100, size=100).tolist(),
            "c": np.random.choice(["a", "b", "c", "d"], size=100).tolist(),
        }
    )

    return df


@pytest.fixture(scope="function")
def db(tmp_path_factory, dataframe):
    dbfile = str(tmp_path_factory.mktemp("db") / "test.duckdb")
    con = duckdb.connect(dbfile)
    con.sql(f"CREATE TABLE {TEMP_TABLE} AS SELECT * FROM dataframe")
    con.close()
    os.environ["TEST_DUCKDB_URI"] = dbfile  # used in catalog default
    return dbfile


@pytest.fixture(scope="function")
def connection(db):
    con = duckdb.connect(db)
    yield con
    con.close()


@pytest.fixture(scope="function")
def duckdb_source(db, connection, dataframe):
    sql_expr = f"SELECT * from {TEMP_TABLE}"
    return DuckDBSource(db, sql_expr)


@pytest.fixture(scope="function")
def duckdb_source_chunked(db, connection, dataframe):
    sql_expr = f"SELECT * from {TEMP_TABLE}"
    return DuckDBSource(db, sql_expr, chunks=10)
