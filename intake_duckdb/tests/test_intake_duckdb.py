from .conftest import TEMP_TABLE


def test_duckdb_source(connection, duckdb_source):
    tables = connection.execute("SHOW TABLES").fetchall()
    assert TEMP_TABLE in [table[0] for table in tables]


def test_read(duckdb_source, dataframe):
    result_df = duckdb_source.read()
    assert result_df.equals(dataframe)
