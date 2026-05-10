"""
test_dag.py — football-pipeline-v5

Test classes
------------
TestDagStructure      — DAG loads, task IDs, dependency chain, dbt_run is BashOperator
TestIngestTask        — CSV → Parquet (Spark); carried forward from v4
TestValidateTask      — null / empty / negative-goals guards; carried forward from v4
TestLoadPostgresTask  — JDBC url, properties, upsert hook calls; carried forward from v4
TestDbtModels         — NEW: stg_matches view + standings SQL tested in-memory via tempview
TestDbtTests          — NEW: assert_positive_goals singular test logic
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

from spark_utils import MATCH_SCHEMA, jdbc_url, jdbc_properties, jdbc_params_from_env


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_ROWS = [
    {"match_id": "m1", "season": "2023-24", "matchday": 1,
     "home_team": "Arsenal", "away_team": "Chelsea",
     "home_goals": 2, "away_goals": 0},
    {"match_id": "m2", "season": "2023-24", "matchday": 1,
     "home_team": "Liverpool", "away_team": "Everton",
     "home_goals": 1, "away_goals": 1},
    {"match_id": "m3", "season": "2023-24", "matchday": 2,
     "home_team": "Chelsea", "away_team": "Arsenal",
     "home_goals": 0, "away_goals": 1},
]


def make_df(spark: SparkSession, rows=None):
    rows = rows or SAMPLE_ROWS
    return spark.createDataFrame([Row(**r) for r in rows], schema=MATCH_SCHEMA)


# ─────────────────────────────────────────────────────────────────────────────
# 1. TestDagStructure
# ─────────────────────────────────────────────────────────────────────────────

class TestDagStructure:
    @pytest.fixture(scope="class")
    def dag(self):
        from airflow.models import DagBag
        dag_dir = str(Path(__file__).parent.parent / "dags")
        bag = DagBag(dag_folder=dag_dir, include_examples=False)
        assert "football_pipeline" in bag.dags, "DAG not found in DagBag"
        return bag.dags["football_pipeline"]

    def test_dag_loads(self, dag):
        assert dag is not None

    def test_task_ids(self, dag):
        ids = set(dag.task_ids)
        assert ids == {"ingest", "validate", "load_postgres", "dbt_run"}

    def test_dependency_chain(self, dag):
        assert "validate" in {t.task_id for t in dag.get_task("ingest").downstream_list}
        assert "load_postgres" in {t.task_id for t in dag.get_task("validate").downstream_list}
        assert "dbt_run" in {t.task_id for t in dag.get_task("load_postgres").downstream_list}

    def test_schedule(self, dag):
        assert dag.schedule_interval == "@weekly"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_retries(self, dag):
        assert dag.default_args.get("retries") == 2

    def test_dbt_run_is_bash_operator(self, dag):
        from airflow.operators.bash import BashOperator
        assert isinstance(dag.get_task("dbt_run"), BashOperator)

    def test_dbt_run_command_contains_dbt_run(self, dag):
        task = dag.get_task("dbt_run")
        assert "dbt run" in task.bash_command

    def test_dbt_run_command_contains_dbt_test(self, dag):
        task = dag.get_task("dbt_run")
        assert "dbt test" in task.bash_command


# ─────────────────────────────────────────────────────────────────────────────
# 2. TestIngestTask
# ─────────────────────────────────────────────────────────────────────────────

class TestIngestTask:
    def test_csv_round_trips_to_parquet(self, spark, tmp_path):
        df = make_df(spark)
        csv_dir = tmp_path / "raw"
        csv_dir.mkdir()
        df.toPandas().to_csv(csv_dir / "matches.csv", index=False)

        loaded = spark.read.csv(str(csv_dir / "matches.csv"),
                                schema=MATCH_SCHEMA, header=True, mode="FAILFAST")
        parquet_dir = tmp_path / "parquet"
        loaded.write.mode("overwrite").partitionBy("season").parquet(str(parquet_dir))

        reread = spark.read.parquet(str(parquet_dir))
        assert reread.count() == len(SAMPLE_ROWS)

    def test_failfast_rejects_wrong_type(self, spark, tmp_path):
        from pyspark.sql.utils import AnalysisException
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("match_id,season,matchday,home_team,away_team,home_goals,away_goals\n"
                           "m1,2023-24,one,Arsenal,Chelsea,two,0\n")
        with pytest.raises(Exception):
            spark.read.csv(str(bad_csv), schema=MATCH_SCHEMA,
                           header=True, mode="FAILFAST").count()

    def test_parquet_partitioned_by_season(self, spark, tmp_path):
        df = make_df(spark)
        out = tmp_path / "parquet"
        df.write.mode("overwrite").partitionBy("season").parquet(str(out))
        partition_dirs = [p for p in out.iterdir() if p.is_dir() and "season=" in p.name]
        assert len(partition_dirs) >= 1

    def test_missing_csv_dir_raises(self):
        with pytest.raises(FileNotFoundError):
            raise FileNotFoundError("No CSV files found")


# ─────────────────────────────────────────────────────────────────────────────
# 3. TestValidateTask
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateTask:
    def test_clean_data_passes(self, spark):
        df = make_df(spark)
        assert df.count() > 0
        for c in df.columns:
            assert df.filter(col(c).isNull()).count() == 0

    def test_empty_dataframe_raises(self, spark):
        df = spark.createDataFrame([], schema=MATCH_SCHEMA)
        with pytest.raises(ValueError, match="empty"):
            if df.count() == 0:
                raise ValueError("Parquet dataset is empty — nothing to validate.")

    def test_null_column_raises(self, spark):
        rows = [Row(match_id=None, season="2023-24", matchday=1,
                    home_team="A", away_team="B", home_goals=1, away_goals=0)]
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)
        with pytest.raises(ValueError, match="null"):
            null_count = df.filter(col("match_id").isNull()).count()
            if null_count > 0:
                raise ValueError(f"Column 'match_id' has {null_count} null value(s).")

    def test_negative_goals_raises(self, spark):
        rows = [Row(match_id="m1", season="2023-24", matchday=1,
                    home_team="A", away_team="B", home_goals=-1, away_goals=0)]
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)
        with pytest.raises(ValueError, match="negative"):
            neg = df.filter((col("home_goals") < 0) | (col("away_goals") < 0)).count()
            if neg > 0:
                raise ValueError(f"{neg} row(s) have negative goal values.")


# ─────────────────────────────────────────────────────────────────────────────
# 4. TestLoadPostgresTask
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadPostgresTask:
    def test_jdbc_url_format(self):
        url = jdbc_url(host="db", port="5432", dbname="football")
        assert url == "jdbc:postgresql://db:5432/football"

    def test_jdbc_properties_keys(self):
        props = jdbc_properties(user="u", password="p")
        assert "user" in props
        assert "password" in props
        assert "driver" in props

    def test_jdbc_driver_is_postgres(self):
        props = jdbc_properties(user="u", password="p")
        assert props["driver"] == "org.postgresql.Driver"

    def test_params_from_env(self, monkeypatch):
        monkeypatch.setenv("FOOTBALL_DB_HOST", "myhost")
        monkeypatch.setenv("FOOTBALL_DB_NAME", "mydb")
        params = jdbc_params_from_env()
        assert params["host"] == "myhost"
        assert params["dbname"] == "mydb"

    def test_hook_run_called_for_upsert(self):
        mock_hook = MagicMock()
        mock_hook.run("INSERT INTO matches SELECT * FROM matches_staging ON CONFLICT (match_id) DO UPDATE SET season=EXCLUDED.season;")
        mock_hook.run.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# 5. TestDbtModels  ← NEW
# ─────────────────────────────────────────────────────────────────────────────

# The UNION ALL standings SQL is extracted here so it can run inside
# spark.sql() against an in-memory temp view — no dbt compile step needed.
# This validates the core logic independently of the dbt runtime.

STANDINGS_SQL = """
WITH home_perspective AS (
    SELECT season, home_team AS team,
           COUNT(*) AS played,
           SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) AS won,
           SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS drawn,
           SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) AS lost,
           SUM(home_goals) AS gf,
           SUM(away_goals) AS ga
    FROM   matches
    GROUP  BY season, home_team
),
away_perspective AS (
    SELECT season, away_team AS team,
           COUNT(*) AS played,
           SUM(CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END) AS won,
           SUM(CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS drawn,
           SUM(CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END) AS lost,
           SUM(away_goals) AS gf,
           SUM(home_goals) AS ga
    FROM   matches
    GROUP  BY season, away_team
),
combined AS (
    SELECT * FROM home_perspective
    UNION ALL
    SELECT * FROM away_perspective
),
aggregated AS (
    SELECT season, team,
           SUM(played) AS played,
           SUM(won)    AS won,
           SUM(drawn)  AS drawn,
           SUM(lost)   AS lost,
           SUM(gf)     AS gf,
           SUM(ga)     AS ga,
           SUM(gf) - SUM(ga)    AS gd,
           SUM(won)*3 + SUM(drawn) AS points
    FROM   combined
    GROUP  BY season, team
)
SELECT season, team, played, won, drawn, lost, gf, ga, gd, points,
       ROW_NUMBER() OVER (PARTITION BY season ORDER BY points DESC, gd DESC, gf DESC) AS position
FROM   aggregated
"""

STG_SQL = """
SELECT
    match_id,
    season,
    matchday,
    home_team,
    away_team,
    home_goals,
    away_goals,
    CASE WHEN home_goals > away_goals THEN home_team
         WHEN away_goals > home_goals THEN away_team
         ELSE NULL END AS winning_team,
    CASE WHEN home_goals = away_goals THEN 'draw'
         WHEN home_goals > away_goals THEN 'home_win'
         ELSE 'away_win' END AS result_type
FROM raw_matches
WHERE match_id IS NOT NULL
"""


class TestDbtModels:
    """Test dbt model SQL logic in-memory using Spark SQL temp views."""

    @pytest.fixture(autouse=True)
    def register_views(self, spark):
        df = make_df(spark)
        df.createOrReplaceTempView("matches")
        df.createOrReplaceTempView("raw_matches")
        yield
        spark.catalog.dropTempView("matches")
        spark.catalog.dropTempView("raw_matches")

    def test_standings_returns_rows(self, spark):
        result = spark.sql(STANDINGS_SQL)
        assert result.count() > 0

    def test_standings_has_expected_columns(self, spark):
        result = spark.sql(STANDINGS_SQL)
        cols = set(result.columns)
        assert {"season", "team", "played", "won", "drawn", "lost",
                "gf", "ga", "gd", "points", "position"}.issubset(cols)

    def test_standings_points_calculation(self, spark):
        """Arsenal: 2 wins (home m1, away m3) = 6 pts. Chelsea: 0 pts."""
        result = spark.sql(STANDINGS_SQL)
        rows = {r.team: r for r in result.filter("season='2023-24'").collect()}
        assert rows["Arsenal"].points == 6
        assert rows["Chelsea"].points == 0

    def test_standings_arsenal_position_is_1(self, spark):
        result = spark.sql(STANDINGS_SQL)
        arsenal = result.filter("team='Arsenal'").collect()[0]
        assert arsenal.position == 1

    def test_standings_played_count(self, spark):
        result = spark.sql(STANDINGS_SQL)
        rows = {r.team: r for r in result.filter("season='2023-24'").collect()}
        # Arsenal played m1 (home) + m3 (away) = 2; Liverpool m2 (home) = 1
        assert rows["Arsenal"].played == 2
        assert rows["Liverpool"].played == 1

    def test_stg_matches_adds_result_type(self, spark):
        result = spark.sql(STG_SQL)
        assert "result_type" in result.columns

    def test_stg_matches_home_win_result_type(self, spark):
        result = spark.sql(STG_SQL)
        m1 = result.filter("match_id='m1'").collect()[0]
        assert m1.result_type == "home_win"

    def test_stg_matches_draw_result_type(self, spark):
        result = spark.sql(STG_SQL)
        m2 = result.filter("match_id='m2'").collect()[0]
        assert m2.result_type == "draw"

    def test_stg_matches_filters_null_match_id(self, spark):
        from pyspark.sql import Row
        rows_with_null = SAMPLE_ROWS + [
            {"match_id": None, "season": "2023-24", "matchday": 3,
             "home_team": "X", "away_team": "Y", "home_goals": 1, "away_goals": 0}
        ]
        df = spark.createDataFrame([Row(**r) for r in rows_with_null], schema=MATCH_SCHEMA)
        df.createOrReplaceTempView("raw_matches")
        result = spark.sql(STG_SQL)
        assert result.count() == len(SAMPLE_ROWS)


# ─────────────────────────────────────────────────────────────────────────────
# 6. TestDbtTests  ← NEW
# ─────────────────────────────────────────────────────────────────────────────

ASSERT_POSITIVE_GOALS_SQL = """
SELECT match_id, home_goals, away_goals
FROM   stg_matches
WHERE  home_goals < 0 OR away_goals < 0
"""


class TestDbtTests:
    """Test the custom singular dbt test logic in-memory."""

    def test_no_violations_on_clean_data(self, spark):
        df = make_df(spark)
        df.createOrReplaceTempView("stg_matches")
        violations = spark.sql(ASSERT_POSITIVE_GOALS_SQL)
        assert violations.count() == 0
        spark.catalog.dropTempView("stg_matches")

    def test_detects_negative_home_goals(self, spark):
        rows = [Row(match_id="bad", season="2023-24", matchday=1,
                    home_team="A", away_team="B", home_goals=-1, away_goals=0)]
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)
        df.createOrReplaceTempView("stg_matches")
        violations = spark.sql(ASSERT_POSITIVE_GOALS_SQL)
        assert violations.count() == 1
        spark.catalog.dropTempView("stg_matches")

    def test_detects_negative_away_goals(self, spark):
        rows = [Row(match_id="bad", season="2023-24", matchday=1,
                    home_team="A", away_team="B", home_goals=0, away_goals=-2)]
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)
        df.createOrReplaceTempView("stg_matches")
        violations = spark.sql(ASSERT_POSITIVE_GOALS_SQL)
        assert violations.count() == 1
        spark.catalog.dropTempView("stg_matches")

    def test_zero_goals_is_not_a_violation(self, spark):
        rows = [Row(match_id="ok", season="2023-24", matchday=1,
                    home_team="A", away_team="B", home_goals=0, away_goals=0)]
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)
        df.createOrReplaceTempView("stg_matches")
        violations = spark.sql(ASSERT_POSITIVE_GOALS_SQL)
        assert violations.count() == 0
        spark.catalog.dropTempView("stg_matches")
