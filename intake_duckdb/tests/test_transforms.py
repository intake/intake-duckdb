import os

import intake
import pandas as pd
import pytest

from .. import DuckDBSource, DuckDBTransform
from .conftest import HERE, TEMP_TABLE


@pytest.fixture(scope="module")
def test_csv(tmp_path_factory, dataframe):
    csvfile = str(tmp_path_factory.mktemp("csv") / "test.csv")
    dataframe.to_csv(csvfile, index=False)
    os.environ["TEST_CSV_FILE"] = csvfile  # used in catalog default
    return csvfile


def test_duckdb_transform(db, dataframe):
    source = DuckDBSource(db, f"SELECT * from {TEMP_TABLE}")
    assert source.read().equals(dataframe)

    source.name = "tablename"
    transform = DuckDBTransform([source], "SELECT * from tablename LIMIT 5")
    transform_df = transform.read()

    assert transform_df.equals(dataframe.head(5))
    assert "tablename" not in globals()


def test_duckdb_transform_join(db, dataframe):
    source1 = DuckDBSource(db, f"SELECT a, b from {TEMP_TABLE}")
    source2 = DuckDBSource(db, f"SELECT b, c from {TEMP_TABLE}")

    source1.name = "table1"
    source2.name = "table2"

    join_query = "SELECT * FROM table1 JOIN table2 ON (table1.b = table2.b)"
    transform = DuckDBTransform([source1, source2], join_query)
    transform_df = transform.read()

    assert isinstance(transform_df, pd.DataFrame)
    assert "table1" not in globals()
    assert "table2" not in globals()


def test_transform_other_catalog_source(db, dataframe, test_csv):
    os.environ["TEST_DUCKDB_URI"] = db
    cat = intake.open_catalog(os.path.join(HERE, "cat.yaml"))

    assert "test_csv_1" in cat
    assert "transform_source_1" in cat

    transform_df = cat.transform_source_1.read()
    assert len(transform_df) == 10


def test_join_multiple_source_types(db, dataframe, test_csv):
    os.environ["TEST_DUCKDB_URI"] = db
    here = os.path.abspath(os.path.dirname(__file__))
    cat = intake.open_catalog(os.path.join(here, "cat.yaml"))

    assert "test_csv_1" in cat
    assert "test_csv_2" in cat
    assert "transform_source_2" in cat

    transform_df = cat.transform_source_2.read()
    assert isinstance(transform_df, pd.DataFrame)
