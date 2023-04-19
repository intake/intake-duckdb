# Building Documentation

Intake-DuckDB uses ReadTheDocs and Sphinx to generate documentation. To build documentation for Intake-DuckDB, first create
an environment from the included `requirements.txt` file.

```bash
pip install -r requirements.txt
```

To make HTML documentation:

```bash
make html
```

Navigate to and open `build/html/index.html`
