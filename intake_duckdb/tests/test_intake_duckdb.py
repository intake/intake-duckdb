import pandas as pd

from .. import DuckDBSource
from .conftest import TEMP_TABLE


def test_db_connection(connection):
    tables = connection.execute("SHOW TABLES").fetchall()
    assert TEMP_TABLE in [table[0] for table in tables]


def test_duckdb_source_full_expression(db, dataframe):
    source = DuckDBSource(db, f"SELECT * from {TEMP_TABLE}")
    assert source.read().equals(dataframe)


def test_duckdb_source_table_name(db, dataframe):
    source = DuckDBSource(db, table=TEMP_TABLE)
    assert source.read().equals(dataframe)


def test_open_duckdb(db):
    import intake

    source = intake.open_duckdb(db, f"SELECT * FROM {TEMP_TABLE}")
    assert isinstance(source, DuckDBSource)


def test_read(duckdb_source, dataframe):
    result_df = duckdb_source.read()
    assert result_df.equals(dataframe)


def test_discover(duckdb_source):
    assert duckdb_source._dataframe is None

    schema = duckdb_source.discover()

    assert schema is not None
    assert duckdb_source._schema is not None

    # discover() should not load entire dataframe
    assert duckdb_source._dataframe is None


def test_read_chunked_from_db(duckdb_source_chunked, dataframe):
    duckdb_source_chunked.discover()
    assert duckdb_source_chunked.npartitions == 10
    assert duckdb_source_chunked._dataframe is None

    df_chunks = []
    for chunk in duckdb_source_chunked.read_chunked():
        df_chunks.append(chunk)

    assert len(df_chunks) == 10
    df = pd.concat(df_chunks, ignore_index=True)

    assert df.equals(dataframe)

    # Reading chunks directly from db shouldn't load dataframe
    assert duckdb_source_chunked._dataframe is None


def test_read_chunked_from_df(duckdb_source_chunked, dataframe):
    duckdb_source_chunked._load()  # load entire dataframe
    assert duckdb_source_chunked.npartitions == 10
    assert duckdb_source_chunked._dataframe is not None

    df_chunks = []
    for chunk in duckdb_source_chunked.read_chunked():
        df_chunks.append(chunk)

    assert len(df_chunks) == 10
    df = pd.concat(df_chunks, ignore_index=True)

    assert df.equals(dataframe)
