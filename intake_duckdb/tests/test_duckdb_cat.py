import os
import random
import subprocess
import time

import intake
import pytest
import requests

from intake_duckdb.catalog import DuckDBCatalog

from .conftest import TEMP_TABLE

here = os.path.abspath(os.path.dirname(__file__))


def test_cat(db, connection, dataframe):
    tables = connection.execute("SHOW TABLES").fetchall()
    assert TEMP_TABLE in [table[0] for table in tables]

    cat = DuckDBCatalog(db, connection)
    assert TEMP_TABLE in cat

    assert TEMP_TABLE in cat

    df = cat[TEMP_TABLE].read()
    assert df.equals(dataframe)


def test_open_cat(db, dataframe):
    cat = intake.open_duckdb_cat(db)
    assert isinstance(cat, DuckDBCatalog)
    assert TEMP_TABLE in cat

    df = cat[TEMP_TABLE].read()
    assert df.equals(dataframe)


def test_yaml_cat(db, dataframe):
    os.environ["TEST_DUCKDB_URI"] = db  # used in catalog default
    cat = intake.open_catalog(os.path.join(here, "cat.yaml"))

    assert "tables" in cat

    cat2 = cat.tables()
    assert isinstance(cat2, DuckDBCatalog)
    assert TEMP_TABLE in list(cat2)

    df = cat.tables[TEMP_TABLE].read()
    assert df.equals(dataframe)


@pytest.fixture()
def remote_db(db):
    pytest.importorskip("tornado")
    pytest.importorskip("msgpack")
    pytest.importorskip("requests")
    port = random.randint(1025, 64000)
    os.environ["TEST_DUCKDB_URI"] = db  # used in catalog default

    fn = os.path.join(here, "cat.yaml")
    cmd = ["intake-server", fn, "-p", f"{port}"]
    proc = subprocess.Popen(cmd, env=os.environ)
    timeout = 5
    while True:
        try:
            if requests.get(f"http://localhost:{port}/v1/info").ok:
                break
        except:
            pass
        time.sleep(0.1)
        timeout -= 0.1
        assert timeout > 0, "Server did not come up"
    yield f"intake://localhost:{port}"

    proc.terminate()
    proc.wait()


def test_with_server(remote_db, dataframe):
    cat = intake.open_catalog(remote_db)
    s = cat.tables[TEMP_TABLE]
    data = s.read()
    assert data.equals(dataframe)
