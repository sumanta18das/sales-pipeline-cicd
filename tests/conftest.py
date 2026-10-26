"""
Shared test setup: one local SparkSession for all tests.

This runs Spark on your laptop / on the GitHub runner. No Databricks needed,
so unit tests are fast, free and run on every Pull Request.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Make Spark workers use the same Python as pytest (avoids version mismatch errors,
# common on Windows and on machines with many Python versions).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("sales-pipeline-unit-tests")
        # Databricks serverless runs with ANSI mode ON, so we test the same way.
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
