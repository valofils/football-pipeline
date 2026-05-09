"""
ingest.py — Load raw CSV match data into a validated PyArrow table.

Demonstrates: pathlib, list comprehensions, csv stdlib, pyarrow schema enforcement.
"""

import csv
from pathlib import Path

import pyarrow as pa


SCHEMA = pa.schema([
    pa.field("match_id",        pa.string(),   nullable=False),
    pa.field("date",            pa.string(),   nullable=False),
    pa.field("season",          pa.int32(),    nullable=False),
    pa.field("home_team",       pa.string(),   nullable=False),
    pa.field("away_team",       pa.string(),   nullable=False),
    pa.field("home_goals",      pa.int32(),    nullable=False),
    pa.field("away_goals",      pa.int32(),    nullable=False),
    pa.field("home_shots",      pa.int32(),    nullable=True),
    pa.field("away_shots",      pa.int32(),    nullable=True),
    pa.field("home_possession", pa.float64(),  nullable=True),
    pa.field("away_possession", pa.float64(),  nullable=True),
    pa.field("stadium",         pa.string(),   nullable=True),
    pa.field("referee",         pa.string(),   nullable=True),
])

INT_FIELDS   = {"season", "home_goals", "away_goals", "home_shots", "away_shots"}
FLOAT_FIELDS = {"home_possession", "away_possession"}


def _coerce_row(row: dict) -> dict:
    """Cast string values from CSV into correct Python types. Returns cleaned dict."""
    coerced = {}
    for key, val in row.items():
        val = val.strip()
        if key in INT_FIELDS:
            coerced[key] = int(val) if val else None
        elif key in FLOAT_FIELDS:
            coerced[key] = float(val) if val else None
        else:
            coerced[key] = val if val else None
    return coerced


def load_csv(path: str | Path) -> pa.Table:
    """
    Read a CSV file of match records and return a validated PyArrow table.

    Args:
        path: Path to the CSV file (str or pathlib.Path).

    Returns:
        pa.Table with schema enforced.

    Raises:
        FileNotFoundError: if path does not exist.
        pa.ArrowInvalid:   if data violates the schema.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [_coerce_row(row) for row in reader]

    if not rows:
        raise ValueError(f"No data found in {path}")

    # Transpose list-of-dicts → dict-of-lists (Arrow's preferred input)
    columns = {field.name: [row.get(field.name) for row in rows] for field in SCHEMA}

    table = pa.table(columns, schema=SCHEMA)

    print(f"[ingest] Loaded {table.num_rows} rows from {path.name}")
    print(f"[ingest] Schema: {[f.name for f in SCHEMA]}")
    return table


def load_all_csvs(directory: str | Path) -> pa.Table:
    """
    Load and concatenate all CSV files found in a directory.

    Args:
        directory: folder to scan for *.csv files.

    Returns:
        Single concatenated pa.Table.
    """
    directory = Path(directory)
    csv_files = sorted(directory.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {directory}")

    tables = [load_csv(f) for f in csv_files]
    combined = pa.concat_tables(tables)
    print(f"[ingest] Combined {len(tables)} file(s) → {combined.num_rows} total rows")
    return combined
