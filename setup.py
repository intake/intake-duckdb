#!/usr/bin/env python

from setuptools import setup

import versioneer

requires = open("requirements.txt").read().strip().split("\n")

setup(
    name="intake-duckdb",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="DuckDB plugin for Intake",
    url="https://github.com/intake/intake-duckdb",
    maintainer="Blake Rosenthal",
    maintainer_email="brosenthal@anaconda.com",
    license="BSD",
    py_modules=["intake_duckdb"],
    packages=["intake_duckdb"],
    package_data={"": ["*.csv", "*.yml", "*.html"]},
    entry_points={
        "intake.drivers": [
            "duckdb = intake_duckdb:DuckDBSource",
        ]
    },
    include_package_data=True,
    install_requires=requires,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    zip_safe=False,
)
