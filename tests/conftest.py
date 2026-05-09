"""
conftest.py
~~~~~~~~~~~
Pytest fixtures for football-pipeline-v4.

SparkSession lifecycle
----------------------
The session-scoped `spark` fixture creates ONE SparkSession for the entire
test run and tears it down at the end. This mirrors how v2 managed the
psycopg2 connection pool — one shared resource, cleaned up once.

Creating a SparkSession per test (function scope) works but is ~5× slower
because JVM startup is expensive. Session scope is the standard pattern for
Spark test suites.

local[2] — run with 2 local threads (parallelism without a real cluster).
           [*] uses all cores; [1] is fully serial — useful for debugging
           task ordering issues.
"""

import os
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Shared SparkSession for the full test run.
    Uses local[2] — no Docker cluster needed for unit tests.
    """
    session = (
        SparkSession.builder
        .appName("football-pipeline-v4-tests")
        .master("local[2]")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.shuffle.partitions", "4")   # small test data → fewer partitions
        .config("spark.ui.enabled", "false")            # suppress Spark UI during tests
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")           # suppress INFO noise in test output
    yield session
    session.stop()


@pytest.fixture(scope="function")
def tmp_dir():
    """Temporary directory auto-cleaned after each test."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture(scope="session")
def sample_rows():
    """
    Minimal match records covering all validation scenarios.
    Returned as plain dicts so tests can build DataFrames from them directly.
    """
    return [
        {
            "match_id": "m001", "date": "2024-08-17", "season": "2024",
            "home_team": "Arsenal",    "away_team": "Wolves",
            "home_goals": 2, "away_goals": 0,
            "home_shots": 14, "away_shots": 6,
            "home_possession": 62.3, "away_possession": 37.7,
            "stadium": "Emirates Stadium", "referee": "M. Oliver",
        },
        {
            "match_id": "m002", "date": "2024-08-17", "season": "2024",
            "home_team": "Chelsea",    "away_team": "Man City",
            "home_goals": 1, "away_goals": 2,
            "home_shots": 9, "away_shots": 13,
            "home_possession": 44.1, "away_possession": 55.9,
            "stadium": "Stamford Bridge", "referee": "A. Taylor",
        },
        {
            "match_id": "m003", "date": "2024-08-18", "season": "2024",
            "home_team": "Liverpool",  "away_team": "Brentford",
            "home_goals": 3, "away_goals": 1,
            "home_shots": 18, "away_shots": 8,
            "home_possession": 67.5, "away_possession": 32.5,
            "stadium": "Anfield", "referee": "S. Attwell",
        },
        {
            "match_id": "m004", "date": "2024-08-18", "season": "2024",
            "home_team": "Tottenham", "away_team": "Leicester",
            "home_goals": 3, "away_goals": 0,
            "home_shots": 16, "away_shots": 5,
            "home_possession": 58.2, "away_possession": 41.8,
            "stadium": "Tottenham Hotspur Stadium", "referee": "C. Kavanagh",
        },
    ]
