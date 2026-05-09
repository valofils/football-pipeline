"""
test_ingest.py — Unit tests for pipeline/ingest.py.

Covers: happy path loading, schema validation, null handling,
        type coercion, missing file error, empty file error,
        multi-file concatenation.
"""

import csv
from pathlib import Path

import pyarrow as pa
import pytest

from pipeline.ingest import load_csv, load_all_csvs, SCHEMA


# ── Happy path ────────────────────────────────────────────────────────────────

class TestLoadCsv:
    def test_returns_arrow_table(self, sample_csv):
        table = load_csv(sample_csv)
        assert isinstance(table, pa.Table)

    def test_row_count(self, sample_csv):
        table = load_csv(sample_csv)
        assert table.num_rows == 6

    def test_schema_matches(self, sample_csv):
        table = load_csv(sample_csv)
        assert table.schema == SCHEMA

    def test_column_names(self, sample_csv):
        table = load_csv(sample_csv)
        expected = [f.name for f in SCHEMA]
        assert table.schema.names == expected

    def test_int_columns_have_correct_type(self, sample_csv):
        table = load_csv(sample_csv)
        assert table.schema.field("home_goals").type == pa.int32()
        assert table.schema.field("season").type     == pa.int32()

    def test_float_columns_have_correct_type(self, sample_csv):
        table = load_csv(sample_csv)
        assert table.schema.field("home_possession").type == pa.float64()

    def test_values_are_correct(self, sample_csv):
        table = load_csv(sample_csv)
        df    = table.to_pandas()
        row   = df[df["match_id"] == "t001"].iloc[0]
        assert row["home_team"]  == "Arsenal"
        assert row["home_goals"] == 2
        assert row["away_goals"] == 0
        assert abs(row["home_possession"] - 62.3) < 0.001


# ── Error handling ────────────────────────────────────────────────────────────

class TestLoadCsvErrors:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_csv(tmp_path / "ghost.csv")

    def test_empty_file_raises(self, tmp_path):
        empty = tmp_path / "empty.csv"
        empty.write_text("match_id,date,season\n", encoding="utf-8")
        with pytest.raises(ValueError, match="No data"):
            load_csv(empty)


# ── Null handling ─────────────────────────────────────────────────────────────

class TestNullHandling:
    def test_nullable_field_accepts_empty(self, tmp_path):
        """home_shots is nullable=True — empty string should become None."""
        content = (
            "match_id,date,season,home_team,away_team,home_goals,away_goals,"
            "home_shots,away_shots,home_possession,away_possession,stadium,referee\n"
            "x001,2024-01-01,2024,A,B,1,0,,,55.0,45.0,,\n"
        )
        p = tmp_path / "nulls.csv"
        p.write_text(content, encoding="utf-8")
        table = load_csv(p)
        import pandas as pd
        val = table.to_pandas()["home_shots"].iloc[0]
        assert pd.isna(val)

    def test_non_nullable_field_still_loads(self, tmp_path):
        """match_id is nullable=False — it must be present."""
        content = (
            "match_id,date,season,home_team,away_team,home_goals,away_goals,"
            "home_shots,away_shots,home_possession,away_possession,stadium,referee\n"
            "x002,2024-01-01,2024,A,B,0,0,,,,,,"
        )
        p = tmp_path / "minimal.csv"
        p.write_text(content, encoding="utf-8")
        table = load_csv(p)
        assert table["match_id"][0].as_py() == "x002"


# ── Multi-file loading ────────────────────────────────────────────────────────

class TestLoadAllCsvs:
    def test_concatenates_multiple_files(self, tmp_path, sample_csv):
        import shutil
        shutil.copy(sample_csv, tmp_path / "a.csv")
        shutil.copy(sample_csv, tmp_path / "b.csv")
        table = load_all_csvs(tmp_path)
        assert table.num_rows == 12   # 6 rows × 2 files

    def test_empty_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_all_csvs(tmp_path)
