# Intake-DuckDB

DuckDB Plugin for Intake

## Installation

```shell
pip install intake-duckdb
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
