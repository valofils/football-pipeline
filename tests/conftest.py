"""
conftest.py — Shared pytest fixtures.

pytest discovers this file automatically. Fixtures defined here are
available to every test file without any import.

Fixture scopes used:
  - session : created once for the whole test run  (DB schema, sample CSV)
  - function: recreated for every test             (fresh DataFrame, clean table)
"""

import csv
import io
from pathlib import Path

import pandas as pd
import psycopg2
import pytest

from pipeline.ingest    import load_csv
from pipeline.transform import enrich
from pipeline.db        import apply_schema, drop_all, load_matches, get_conn


# ── Connection params ─────────────────────────────────────────────────────────
# Tests run against the same DB but in an isolated table state managed
# by the db_schema fixture below.

TEST_DB_PARAMS = {
    "dbname":   "football_db",
    "user":     "football",
    "password": "football123",
    "host":     "localhost",
    "port":     5432,
}


@pytest.fixture(scope="session")
def db_params():
    """Return the test database connection parameters."""
    return TEST_DB_PARAMS


# ── Minimal CSV content ───────────────────────────────────────────────────────

SAMPLE_CSV_CONTENT = """\
match_id,date,season,home_team,away_team,home_goals,away_goals,home_shots,away_shots,home_possession,away_possession,stadium,referee
t001,2024-08-17,2024,Arsenal,Wolves,2,0,14,6,62.3,37.7,Emirates Stadium,M. Oliver
t002,2024-08-17,2024,Chelsea,Man City,1,2,9,13,44.1,55.9,Stamford Bridge,A. Taylor
t003,2024-08-18,2024,Liverpool,Brentford,3,1,18,8,67.5,32.5,Anfield,S. Attwell
t004,2024-09-01,2024,Arsenal,Tottenham,1,0,11,8,53.6,46.4,Emirates Stadium,M. Oliver
t005,2024-09-14,2024,Chelsea,Wolves,3,2,14,9,50.1,49.9,Stamford Bridge,M. Oliver
t006,2024-09-22,2024,Arsenal,Leicester,4,2,20,8,65.5,34.5,Emirates Stadium,A. Taylor
"""


@pytest.fixture(scope="session")
def sample_csv(tmp_path_factory) -> Path:
    """
    Write the sample CSV to a temp file once per session.
    tmp_path_factory is pytest's session-scoped temp dir factory.
    """
    p = tmp_path_factory.mktemp("data") / "matches.csv"
    p.write_text(SAMPLE_CSV_CONTENT, encoding="utf-8")
    return p


@pytest.fixture(scope="session")
def raw_table(sample_csv):
    """Load the sample CSV into a PyArrow table once per session."""
    return load_csv(sample_csv)


@pytest.fixture
def enriched_df(raw_table) -> pd.DataFrame:
    """
    Return a fresh enriched DataFrame for each test.
    Function-scoped so tests can mutate it without affecting each other.
    """
    return enrich(raw_table)


# ── Database fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def db_schema(db_params):
    """
    Apply the schema once per session, then drop everything after all
    tests finish. Ensures a clean slate regardless of previous runs.
    """
    drop_all(db_params)
    apply_schema(db_params)
    yield
    drop_all(db_params)


@pytest.fixture
def empty_db(db_schema, db_params):
    """
    Truncate the matches table before each test that needs an empty DB.
    Much faster than dropping/recreating the schema every time.
    """
    with get_conn(db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE matches RESTART IDENTITY CASCADE;")
    yield db_params


@pytest.fixture
def loaded_db(empty_db, enriched_df, db_params):
    """
    Provide a DB that already has the sample data loaded.
    Builds on empty_db so the table is always clean before loading.
    """
    load_matches(enriched_df, db_params)
    yield db_params
