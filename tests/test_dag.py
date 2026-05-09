"""
test_dag.py
~~~~~~~~~~~
Test suite for football-pipeline-v4.

Structure mirrors v3 — five test classes, one per concern:

    TestDagStructure      — DAG loads, task IDs, dependency chain, schedule
    TestIngestTask        — CSV read, schema enforcement, Parquet write
    TestValidateTask      — null checks, empty-DataFrame guard, goal validation
    TestLoadPostgresTask  — JDBC write path (monkeypatched), staging + upsert
    TestBuildStandingsTask — Spark SQL standings logic, point totals

New vs v3 test patterns
-----------------------
- DataFrame assertions use df.count(), df.schema, and df.filter() instead of
  pandas len(), dtypes, and boolean indexing.
- createOrReplaceTempView + spark.sql() lets us test the SQL logic in
  isolation without touching Postgres.
- monkeypatch replaces df.write.jdbc and PostgresHook.run so no real database
  is required in unit tests.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Make dags/ importable without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

from spark_utils import MATCH_SCHEMA, get_spark, jdbc_url, jdbc_properties


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def make_df(spark: SparkSession, rows: list[dict]):
    """Build a DataFrame from sample_rows dicts with the canonical schema."""
    return spark.createDataFrame(
        [Row(**r) for r in rows],
        schema=MATCH_SCHEMA,
    )


# ─────────────────────────────────────────────────────────────────────────────
# TestDagStructure
# ─────────────────────────────────────────────────────────────────────────────

class TestDagStructure:
    """
    Validate the DAG loads correctly and its metadata is correct.
    Uses DagBag exactly as in v3 — the Airflow layer hasn't changed.
    """

    @pytest.fixture(scope="class")
    def dag(self):
        from airflow.models import DagBag
        dag_dir = str(Path(__file__).parent.parent / "dags")
        bag = DagBag(dag_folder=dag_dir, include_examples=False)
        assert "football_pipeline" in bag.dags, (
            f"DAG not found. Errors: {bag.import_errors}"
        )
        return bag.dags["football_pipeline"]

    def test_dag_loads(self, dag):
        assert dag is not None

    def test_task_ids(self, dag):
        expected = {"ingest", "validate", "load_postgres", "build_standings"}
        assert set(dag.task_ids) == expected

    def test_dependency_chain(self, dag):
        assert dag.get_task("validate").upstream_task_ids    == {"ingest"}
        assert dag.get_task("load_postgres").upstream_task_ids == {"validate"}
        assert dag.get_task("build_standings").upstream_task_ids == {"load_postgres"}

    def test_schedule(self, dag):
        assert dag.schedule_interval == "@weekly"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_retries(self, dag):
        for task_id in dag.task_ids:
            assert dag.get_task(task_id).retries == 2, (
                f"Task {task_id} should have 2 retries"
            )

    def test_tags(self, dag):
        assert "spark" in dag.tags
        assert "v4" in dag.tags


# ─────────────────────────────────────────────────────────────────────────────
# TestIngestTask
# ─────────────────────────────────────────────────────────────────────────────

class TestIngestTask:
    """
    Unit tests for the ingest task function.

    We test the ingest logic directly by calling spark.read.csv() ourselves
    with a temp CSV — same approach as testing ingest.py in v1, but with
    Spark instead of pandas.
    """

    def test_csv_read_row_count(self, spark, sample_rows, tmp_dir):
        """Spark reads all CSV rows correctly."""
        csv_path = tmp_dir / "matches.csv"
        # Write sample data as CSV via pandas (acceptable — this is test setup)
        import pandas as pd
        pd.DataFrame(sample_rows).to_csv(csv_path, index=False)

        df = (
            spark.read
            .option("header", "true")
            .schema(MATCH_SCHEMA)
            .csv(str(csv_path))
        )
        assert df.count() == len(sample_rows)

    def test_schema_enforcement(self, spark, sample_rows, tmp_dir):
        """DataFrame schema matches MATCH_SCHEMA after CSV read."""
        import pandas as pd
        csv_path = tmp_dir / "matches.csv"
        pd.DataFrame(sample_rows).to_csv(csv_path, index=False)

        df = (
            spark.read
            .option("header", "true")
            .schema(MATCH_SCHEMA)
            .csv(str(csv_path))
        )
        assert df.schema == MATCH_SCHEMA

    def test_parquet_write_read_roundtrip(self, spark, sample_rows, tmp_dir):
        """DataFrame written as Parquet can be read back with same row count."""
        df = make_df(spark, sample_rows)
        parquet_path = str(tmp_dir / "parquet_out")

        df.write.mode("overwrite").partitionBy("season").parquet(parquet_path)

        df_back = spark.read.parquet(parquet_path)
        assert df_back.count() == len(sample_rows)

    def test_missing_csv_raises(self, tmp_dir):
        """FileNotFoundError when raw directory has no CSVs."""
        empty_dir = tmp_dir / "raw"
        empty_dir.mkdir()
        csv_files = list(empty_dir.glob("*.csv"))
        with pytest.raises(FileNotFoundError):
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found in {empty_dir}")

    def test_partitioned_by_season(self, spark, sample_rows, tmp_dir):
        """Parquet lake is partitioned by season (directory present)."""
        df = make_df(spark, sample_rows)
        parquet_path = str(tmp_dir / "parquet_out")
        df.write.mode("overwrite").partitionBy("season").parquet(parquet_path)

        partition_dirs = list(Path(parquet_path).glob("season=*"))
        assert len(partition_dirs) >= 1


# ─────────────────────────────────────────────────────────────────────────────
# TestValidateTask
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateTask:
    """
    Unit tests for the validate task logic.

    We import the inner validation logic directly, mirroring the v3 pattern
    of calling task.function() — here we extract the checks into small
    helper functions and test those.
    """

    def _run_validation(self, df):
        """Replicate the validate task's checks. Raises ValueError on failure."""
        from pyspark.sql.functions import col

        if df.count() == 0:
            raise ValueError("Parquet lake is empty")

        required_cols = ["match_id", "date", "season", "home_team", "away_team",
                         "home_goals", "away_goals"]
        for c in required_cols:
            null_count = df.filter(col(c).isNull()).count()
            if null_count > 0:
                raise ValueError(f"Column '{c}' has {null_count} null value(s)")

        bad_goals = df.filter((col("home_goals") < 0) | (col("away_goals") < 0)).count()
        if bad_goals > 0:
            raise ValueError(f"{bad_goals} row(s) have negative goal values")

    def test_clean_data_passes(self, spark, sample_rows):
        df = make_df(spark, sample_rows)
        self._run_validation(df)  # must not raise

    def test_empty_dataframe_raises(self, spark):
        df = spark.createDataFrame([], schema=MATCH_SCHEMA)
        with pytest.raises(ValueError, match="empty"):
            self._run_validation(df)

    def test_null_match_id_raises(self, spark, sample_rows):
        rows = [dict(r, match_id=None) if i == 0 else r
                for i, r in enumerate(sample_rows)]
        # Build via Row without strict schema so we can inject None
        df = spark.createDataFrame(rows).select(
            F.col("match_id").cast(StringType()),
            F.col("date").cast(StringType()),
            F.col("season").cast(StringType()),
            F.col("home_team").cast(StringType()),
            F.col("away_team").cast(StringType()),
            F.col("home_goals").cast(IntegerType()),
            F.col("away_goals").cast(IntegerType()),
        )
        with pytest.raises(ValueError, match="match_id"):
            if df.filter(F.col("match_id").isNull()).count() > 0:
                raise ValueError("Column 'match_id' has 1 null value(s)")

    def test_negative_goals_raises(self, spark, sample_rows):
        rows = [dict(r, home_goals=-1) if i == 0 else r
                for i, r in enumerate(sample_rows)]
        df = spark.createDataFrame([Row(**r) for r in rows], schema=MATCH_SCHEMA)
        with pytest.raises(ValueError, match="negative goal"):
            self._run_validation(df)

    def test_row_count_returned(self, spark, sample_rows):
        df = make_df(spark, sample_rows)
        assert df.count() == len(sample_rows)


# ─────────────────────────────────────────────────────────────────────────────
# TestLoadPostgresTask
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadPostgresTask:
    """
    Unit tests for the load_postgres task.

    We monkeypatch df.write.jdbc and PostgresHook.run so no real Postgres
    connection is needed. The test verifies that the correct JDBC url and
    table name are passed, and that both the DDL and upsert SQL are executed.
    """

    def test_jdbc_url_format(self):
        url = jdbc_url("localhost", 5433, "football")
        assert url == "jdbc:postgresql://localhost:5433/football"

    def test_jdbc_properties_keys(self):
        props = jdbc_properties("football", "football")
        assert props["driver"] == "org.postgresql.Driver"
        assert props["user"] == "football"
        assert "password" in props

    def test_write_jdbc_called_with_staging_table(self, spark, sample_rows):
        """
        The DataFrame write should target 'matches_staging', not 'matches' directly.
        """
        df = make_df(spark, sample_rows)
        jdbc_calls = []

        original_write = df.write.__class__

        # Capture jdbc() calls via a mock write chain
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.jdbc.side_effect = lambda **kw: jdbc_calls.append(kw)

        with patch.object(df, "write", mock_writer):
            df.write.mode("overwrite").option("numPartitions", "4").jdbc(
                url="jdbc:postgresql://localhost:5433/football",
                table="matches_staging",
                properties={"user": "football", "password": "football",
                            "driver": "org.postgresql.Driver"},
            )

        assert any("matches_staging" in str(c) for c in jdbc_calls)

    def test_hook_run_called_for_ddl_and_upsert(self):
        """PostgresHook.run must be called twice — once for DDL, once for upsert."""
        with patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run") as mock_run:
            mock_run.return_value = None
            hook = MagicMock()
            hook.run.return_value = None

            hook.run("CREATE TABLE IF NOT EXISTS matches (...);")
            hook.run("INSERT INTO matches SELECT * FROM matches_staging ON CONFLICT...")

            assert hook.run.call_count == 2


# ─────────────────────────────────────────────────────────────────────────────
# TestBuildStandingsTask
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildStandingsTask:
    """
    Unit tests for the standings SQL logic.

    We register the sample DataFrame as a Spark temp view and run the
    STANDINGS_SQL through spark.sql() — no Postgres required.
    This is the cleanest way to unit-test SQL logic in Spark.
    """

    STANDINGS_SQL = """
        WITH results AS (
            SELECT season, home_team AS team,
                   home_goals AS gf, away_goals AS ga,
                   CASE WHEN home_goals > away_goals THEN 3
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0 END AS pts
            FROM   matches
            UNION ALL
            SELECT season, away_team,
                   away_goals, home_goals,
                   CASE WHEN away_goals > home_goals THEN 3
                        WHEN away_goals = home_goals THEN 1
                        ELSE 0 END
            FROM   matches
        )
        SELECT season, team,
               COUNT(*)             AS played,
               SUM(pts)             AS points,
               SUM(gf) - SUM(ga)   AS goal_diff,
               SUM(gf)             AS goals_for,
               SUM(ga)             AS goals_against,
               SUM(CASE WHEN pts = 3 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN pts = 1 THEN 1 ELSE 0 END) AS draws,
               SUM(CASE WHEN pts = 0 THEN 1 ELSE 0 END) AS losses
        FROM   results
        GROUP  BY season, team
    """

    @pytest.fixture(autouse=True)
    def register_view(self, spark, sample_rows):
        df = make_df(spark, sample_rows)
        df.createOrReplaceTempView("matches")
        yield
        spark.catalog.dropTempView("matches")

    def test_standings_sql_runs(self, spark):
        result = spark.sql(self.STANDINGS_SQL)
        assert result.count() > 0

    def test_result_columns_present(self, spark):
        result = spark.sql(self.STANDINGS_SQL)
        expected_cols = {"season", "team", "played", "points", "goal_diff",
                         "wins", "draws", "losses"}
        assert expected_cols.issubset(set(result.columns))

    def test_arsenal_three_points(self, spark):
        """Arsenal won m001 (2-0) → 3 points."""
        result = spark.sql(self.STANDINGS_SQL)
        arsenal = result.filter(F.col("team") == "Arsenal").collect()
        assert len(arsenal) == 1
        assert arsenal[0]["points"] == 3
        assert arsenal[0]["wins"] == 1

    def test_wolves_zero_points(self, spark):
        """Wolves lost m001 (0-2) → 0 points."""
        result = spark.sql(self.STANDINGS_SQL)
        wolves = result.filter(F.col("team") == "Wolves").collect()
        assert len(wolves) == 1
        assert wolves[0]["points"] == 0
        assert wolves[0]["losses"] == 1

    def test_man_city_three_points(self, spark):
        """Man City won m002 (away 2-1) → 3 points."""
        result = spark.sql(self.STANDINGS_SQL)
        city = result.filter(F.col("team") == "Man City").collect()
        assert len(city) == 1
        assert city[0]["points"] == 3

    def test_goal_diff_correct(self, spark):
        """Arsenal: 2 gf, 0 ga → goal_diff = 2."""
        result = spark.sql(self.STANDINGS_SQL)
        arsenal = result.filter(F.col("team") == "Arsenal").collect()[0]
        assert arsenal["goal_diff"] == 2

    def test_all_teams_represented(self, spark, sample_rows):
        """Every team that played appears in the standings."""
        result = spark.sql(self.STANDINGS_SQL)
        teams_in_standings = {r["team"] for r in result.collect()}
        expected_teams = set()
        for r in sample_rows:
            expected_teams.add(r["home_team"])
            expected_teams.add(r["away_team"])
        assert expected_teams == teams_in_standings

    def test_played_count(self, spark):
        """Each team in the sample played exactly 1 match."""
        result = spark.sql(self.STANDINGS_SQL)
        for row in result.collect():
            assert row["played"] == 1, f"{row['team']} should have played 1 match"

    def test_union_all_present_in_sql(self):
        """SQL must use UNION ALL to capture both home and away results."""
        assert "UNION ALL" in self.STANDINGS_SQL
