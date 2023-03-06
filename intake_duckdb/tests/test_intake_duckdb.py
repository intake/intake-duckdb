from .. import DuckDBSource
from .conftest import TEMP_TABLE


def test_duckdb_source(connection, duckdb_source):
    tables = connection.execute("SHOW TABLES").fetchall()
    assert TEMP_TABLE in [table[0] for table in tables]


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
