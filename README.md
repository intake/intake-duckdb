# Intake-DuckDB

[![Build Status](https://github.com/intake/intake-duckdb//actions/workflows/main.yaml/badge.svg)](https://github.com/intake/intake-duckdb/actions)
[![Documentation Status](https://readthedocs.org/projects/intake-duckdb/badge/?version=latest)](http://intake-duckdb.readthedocs.io/en/latest/?badge=latest)

DuckDB Plugin for Intake

## Installation

From PyPI
```shell
pip install intake-duckdb
```

Or conda-forge
```shell
conda install -c conda-forge intake-duckdb
```
## Usage

Load an entire table into a dataframe
```python
source = intake.open_duckdb("path/to/dbfile", "tablename")
df = source.read()

```
Or a custom SQL in [valid DuckDB query syntax](https://duckdb.org/docs/sql/query_syntax/select)
```python
source = intake.open_duckdb("path/to/dbfile", "SELECT col1, col2 FROM tablename")
df = source.read()
```

Can also iterate over table chunks
```python
source_chunked = intake.open_duckdb("path/to/dbfile", "tablename", chunks=10)
source_chunked.discover()
for chunk in source_chunked.read_chunked():
    # do something
    ...
```

DuckDB catalog: create an Intake catalog from a DuckDB backend
```python
cat = intake.open_duckdb_cat("path/to/dbfile")

# list the sources in 'cat'
list(cat)

df = cat["tablename"].read()
df_chunks = [chunk for chunk in cat["tablename"](chunks=10).read_chunked()]
```

Run DuckDB queries on other Intake sources (that produce pandas DataFrames) within the same catalog
```yaml
# cat.yaml
sources:
  csv_source:
    args:
      urlpath: https://data.csv
    description: Remote CSV source
    driver: csv

  duck_source:
    args:
      targets:
        - csv_source
      sql_expr: SELECT col FROM csv_source LIMIT 10
    description: Source referencing other sources in catalog
    driver: duckdb_transform
```
```python
cat  = intake.open_catalog("cat.yaml")
duck_source = cat.duck_source.read()
```
