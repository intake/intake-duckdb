- [ ] Read arbitrary SQL from duckdb file into dataframe
- [ ] Read entire table from duckdb file into dataframe
	- Should probably just be a subclass of arbitrary SQL class
 - [ ] Should handle duckdb files and exports
 - [ ] Also handle SQLite files in addition to duckdb?
 - [ ] Transform: derived class for returning duckdb objs? What kind?
 - [ ] Persist method for duckdb objs?
 - [ ] Plots! Use native engine compute?
       - DuckDB has a remote httpfs extension for loading remote parquets and csvs, and can do plots directly with the engine rather than load data into memory https://duckdb.org/docs/guides/python/jupyter
 - [x] Use `pyproject.toml`? Can this do entry points?

All classes should handle:
- [ ] Chunked
- [ ] Not chunked
- [ ] Remote [link](https://duckdb.org/docs/api/python/reference/#duckdb.DuckDBPyConnection.register_filesystem)
