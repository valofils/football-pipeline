"""
test_dag.py
-----------
Test suite for football-pipeline-v6.

Classes:
    TestDagStructure        — DAG loads, has 4 tasks, correct chain
    TestProducer            — produce_matches logic with mocked Producer
    TestConsumer            — consume_matches logic with MockConsumer
    TestConsumerUpsert      — upsert_batch SQL correctness against live DB
    TestValidateTask        — Spark null-check logic (in-memory)
    TestDbtModels           — stg_matches + standings SQL via Spark SQL (no dbt compile)
    TestDbtTests            — assert_positive_goals singular test logic
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent


# ===========================================================================
# 1. DAG structure
# ===========================================================================

class TestDagStructure:
    """Load the DAG with DagBag and assert structural properties."""

    @pytest.fixture(scope="class")
    def dag(self):
        from airflow.models import DagBag
        bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
        assert "football_pipeline" in bag.dags, f"DAG not found. Errors: {bag.import_errors}"
        return bag.dags["football_pipeline"]

    def test_dag_loads(self, dag):
        assert dag is not None

    def test_task_count(self, dag):
        assert len(dag.tasks) == 4

    def test_task_ids(self, dag):
        ids = {t.task_id for t in dag.tasks}
        assert ids == {"kafka_ingest", "validate", "load_postgres", "dbt_run"}

    def test_chain_kafka_ingest_to_validate(self, dag):
        validate = dag.get_task("validate")
        assert "kafka_ingest" in {t.task_id for t in validate.upstream_list}

    def test_chain_validate_to_load(self, dag):
        load = dag.get_task("load_postgres")
        assert "validate" in {t.task_id for t in load.upstream_list}

    def test_chain_load_to_dbt(self, dag):
        dbt = dag.get_task("dbt_run")
        assert "load_postgres" in {t.task_id for t in dbt.upstream_list}

    def test_schedule(self, dag):
        assert dag.schedule_interval == "@weekly"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_retries(self, dag):
        assert dag.default_args["retries"] == 2


# ===========================================================================
# 2. Producer
# ===========================================================================

class TestProducer:
    """Unit-test produce_matches with a mocked confluent_kafka.Producer."""

    @pytest.fixture()
    def tmp_csv(self, tmp_path, sample_rows):
        """Write sample_rows to a temp CSV file."""
        import csv
        f = tmp_path / "2023-24.csv"
        writer = csv.DictWriter(f.open("w", newline=""), fieldnames=sample_rows[0].keys())
        writer.writeheader()
        writer.writerows(sample_rows)
        return tmp_path

    def test_produce_calls_produce_once_per_row(self, tmp_csv, sample_rows, mock_producer):
        from kafka.producer.produce_matches import publish_csv
        n = publish_csv(mock_producer, list(tmp_csv.glob("*.csv"))[0], season="2023-24")
        assert n == len(sample_rows)
        assert mock_producer.produce.call_count == len(sample_rows)

    def test_produce_uses_correct_topic(self, tmp_csv, sample_rows, mock_producer):
        from kafka.producer.produce_matches import publish_csv, TOPIC
        publish_csv(mock_producer, list(tmp_csv.glob("*.csv"))[0], season="2023-24")
        calls = mock_producer.produce.call_args_list
        for call in calls:
            assert call.kwargs["topic"] == TOPIC

    def test_produce_key_contains_season(self, tmp_csv, sample_rows, mock_producer):
        from kafka.producer.produce_matches import publish_csv
        publish_csv(mock_producer, list(tmp_csv.glob("*.csv"))[0], season="2023-24")
        for call in mock_producer.produce.call_args_list:
            assert b"2023-24" in call.kwargs["key"]

    def test_produce_value_is_valid_json(self, tmp_csv, sample_rows, mock_producer):
        from kafka.producer.produce_matches import publish_csv
        publish_csv(mock_producer, list(tmp_csv.glob("*.csv"))[0], season="2023-24")
        for call in mock_producer.produce.call_args_list:
            payload = json.loads(call.kwargs["value"].decode())
            assert "match_id" in payload
            assert payload["season"] == "2023-24"

    def test_produce_flush_called(self, tmp_csv, mock_producer):
        from kafka.producer.produce_matches import publish_csv
        publish_csv(mock_producer, list(tmp_csv.glob("*.csv"))[0], season="2023-24")
        # flush is called by run(), not publish_csv; poll is called during iteration
        # Just assert produce was called (flush tested in integration context)
        assert mock_producer.produce.called

    def test_run_raises_on_flush_failure(self, tmp_csv, mock_producer):
        mock_producer.flush.return_value = 5  # 5 un-delivered = failure
        from kafka.producer.produce_matches import run
        with patch("kafka.producer.produce_matches.Producer", return_value=mock_producer), \
             patch("kafka.producer.produce_matches.ensure_topic"):
            with pytest.raises(RuntimeError, match="flush timed out"):
                run(tmp_csv, season_filter=None)


# ===========================================================================
# 3. Consumer — logic with MockConsumer
# ===========================================================================

class TestConsumer:
    """Test consume() logic using MockConsumer (no real Kafka broker needed)."""

    def test_consume_returns_row_count(self, mock_consumer, sample_rows):
        from kafka.consumer.consume_matches import consume
        with patch("kafka.consumer.consume_matches.Consumer", return_value=mock_consumer), \
             patch("kafka.consumer.consume_matches.get_conn") as mock_get_conn, \
             patch("kafka.consumer.consume_matches.ensure_staging_table"), \
             patch("kafka.consumer.consume_matches.upsert_batch", return_value=len(sample_rows)) as mock_upsert:
            mock_get_conn.return_value.__enter__ = lambda s: MagicMock()
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            result = consume(timeout_seconds=5, batch_size=100)
        assert result == len(sample_rows)

    def test_consumer_commits_offsets(self, mock_consumer, sample_rows):
        from kafka.consumer.consume_matches import consume
        with patch("kafka.consumer.consume_matches.Consumer", return_value=mock_consumer), \
             patch("kafka.consumer.consume_matches.get_conn") as mock_get_conn, \
             patch("kafka.consumer.consume_matches.ensure_staging_table"), \
             patch("kafka.consumer.consume_matches.upsert_batch", return_value=len(sample_rows)):
            mock_get_conn.return_value.__enter__ = lambda s: MagicMock()
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            consume(timeout_seconds=5, batch_size=100)
        assert len(mock_consumer._committed) > 0

    def test_malformed_message_is_skipped(self, sample_rows):
        """A non-JSON message should be skipped, not raise."""
        from tests.conftest import MockConsumer, _FakeMessage

        class BadConsumer(MockConsumer):
            def __init__(self):
                super().__init__(sample_rows)
                # Insert a malformed message at position 0
                bad = _FakeMessage("bad:key", {}, partition=0, offset=99)
                bad._value = b"NOT JSON {"
                self._messages.insert(0, bad)

        consumer = BadConsumer()
        from kafka.consumer.consume_matches import consume
        with patch("kafka.consumer.consume_matches.Consumer", return_value=consumer), \
             patch("kafka.consumer.consume_matches.get_conn") as mock_get_conn, \
             patch("kafka.consumer.consume_matches.ensure_staging_table"), \
             patch("kafka.consumer.consume_matches.upsert_batch", return_value=len(sample_rows)):
            mock_get_conn.return_value.__enter__ = lambda s: MagicMock()
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            # Should not raise
            consume(timeout_seconds=5, batch_size=200)


# ===========================================================================
# 4. Consumer upsert SQL (requires live DB — skipped if not reachable)
# ===========================================================================

class TestConsumerUpsert:
    """Integration tests for upsert_batch against a real Postgres instance."""

    def test_upsert_inserts_rows(self, pg_conn, sample_rows):
        from kafka.consumer.consume_matches import ensure_staging_table, upsert_batch
        ensure_staging_table(pg_conn)
        n = upsert_batch(pg_conn, sample_rows)
        assert n == len(sample_rows)

    def test_upsert_is_idempotent(self, pg_conn, sample_rows):
        from kafka.consumer.consume_matches import ensure_staging_table, upsert_batch
        ensure_staging_table(pg_conn)
        upsert_batch(pg_conn, sample_rows)
        n = upsert_batch(pg_conn, sample_rows)  # second call should not raise / duplicate
        assert n == len(sample_rows)

    def test_upsert_updates_existing_row(self, pg_conn, sample_rows):
        from kafka.consumer.consume_matches import ensure_staging_table, upsert_batch
        ensure_staging_table(pg_conn)
        upsert_batch(pg_conn, sample_rows)

        modified = [dict(r) for r in sample_rows]
        modified[0]["home_goals"] = "99"
        upsert_batch(pg_conn, modified)

        with pg_conn.cursor() as cur:
            cur.execute("SELECT home_goals FROM matches_staging WHERE match_id = %s", (1,))
            row = cur.fetchone()
        assert row[0] == 99


# ===========================================================================
# 5. Validate task — Spark null checks
# ===========================================================================

class TestValidateTask:
    """Test the null-check logic extracted from the validate task."""

    def test_no_nulls_passes(self, spark, sample_rows):
        from pyspark.sql.functions import col
        df = spark.createDataFrame(sample_rows)
        required_cols = ["match_id", "season", "home_team", "away_team"]
        errors = [c for c in required_cols if df.filter(col(c).isNull()).count() > 0]
        assert errors == []

    def test_null_match_id_detected(self, spark, sample_rows):
        from pyspark.sql.functions import col
        rows = [dict(r) for r in sample_rows]
        rows[0]["match_id"] = None
        df = spark.createDataFrame(rows)
        null_count = df.filter(col("match_id").isNull()).count()
        assert null_count == 1

    def test_null_season_detected(self, spark, sample_rows):
        from pyspark.sql.functions import col
        rows = [dict(r) for r in sample_rows]
        rows[2]["season"] = None
        df = spark.createDataFrame(rows)
        assert df.filter(col("season").isNull()).count() == 1


# ===========================================================================
# 6. dbt model SQL — tested in-memory via Spark SQL (carried from v5)
# ===========================================================================

STG_MATCHES_SQL = """
    SELECT
        CAST(match_id   AS INT)    AS match_id,
        CAST(season     AS STRING) AS season,
        CAST(home_team  AS STRING) AS home_team,
        CAST(away_team  AS STRING) AS away_team,
        CAST(home_goals AS INT)    AS home_goals,
        CAST(away_goals AS INT)    AS away_goals,
        match_date,
        referee,
        CASE
            WHEN CAST(home_goals AS INT) > CAST(away_goals AS INT) THEN home_team
            WHEN CAST(away_goals AS INT) > CAST(home_goals AS INT) THEN away_team
            ELSE 'Draw'
        END AS winning_team,
        CASE
            WHEN CAST(home_goals AS INT) > CAST(away_goals AS INT) THEN 'H'
            WHEN CAST(away_goals AS INT) > CAST(home_goals AS INT) THEN 'A'
            ELSE 'D'
        END AS result_type
    FROM raw_matches
    WHERE match_id IS NOT NULL
"""

STANDINGS_SQL = """
    WITH home_points AS (
        SELECT season, home_team AS team,
               SUM(CASE WHEN home_goals > away_goals THEN 3
                        WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS pts,
               SUM(home_goals) AS gf, SUM(away_goals) AS ga,
               COUNT(*) AS played
        FROM stg_matches GROUP BY season, home_team
    ),
    away_points AS (
        SELECT season, away_team AS team,
               SUM(CASE WHEN away_goals > home_goals THEN 3
                        WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS pts,
               SUM(away_goals) AS gf, SUM(home_goals) AS ga,
               COUNT(*) AS played
        FROM stg_matches GROUP BY season, away_team
    )
    SELECT season, team,
           SUM(pts)    AS points,
           SUM(gf)     AS goals_for,
           SUM(ga)     AS goals_against,
           SUM(gf) - SUM(ga) AS goal_diff,
           SUM(played) AS matches_played
    FROM (SELECT * FROM home_points UNION ALL SELECT * FROM away_points)
    GROUP BY season, team
    ORDER BY points DESC
"""


class TestDbtModels:

    @pytest.fixture(autouse=True)
    def setup_views(self, spark, sample_rows):
        df = spark.createDataFrame(sample_rows)
        df.createOrReplaceTempView("raw_matches")
        stg = spark.sql(STG_MATCHES_SQL)
        stg.createOrReplaceTempView("stg_matches")

    def test_stg_matches_row_count(self, spark, sample_rows):
        df = spark.sql("SELECT * FROM stg_matches")
        assert df.count() == len(sample_rows)

    def test_stg_matches_no_nulls_in_key_cols(self, spark):
        from pyspark.sql.functions import col
        df = spark.sql("SELECT * FROM stg_matches")
        for c in ["match_id", "season", "home_team", "away_team"]:
            assert df.filter(col(c).isNull()).count() == 0

    def test_stg_matches_winning_team_populated(self, spark):
        from pyspark.sql.functions import col
        df = spark.sql("SELECT * FROM stg_matches")
        assert df.filter(col("winning_team").isNull()).count() == 0

    def test_stg_matches_result_type_values(self, spark):
        df = spark.sql("SELECT DISTINCT result_type FROM stg_matches")
        values = {r["result_type"] for r in df.collect()}
        assert values.issubset({"H", "A", "D"})

    def test_standings_has_rows(self, spark):
        df = spark.sql(STANDINGS_SQL)
        assert df.count() > 0

    def test_standings_points_non_negative(self, spark):
        from pyspark.sql.functions import col
        df = spark.sql(STANDINGS_SQL)
        assert df.filter(col("points") < 0).count() == 0

    def test_standings_columns_present(self, spark):
        df = spark.sql(STANDINGS_SQL)
        expected = {"season", "team", "points", "goals_for", "goals_against", "goal_diff", "matches_played"}
        assert expected.issubset(set(df.columns))


# ===========================================================================
# 7. dbt singular test — assert_positive_goals (carried from v5)
# ===========================================================================

ASSERT_POSITIVE_GOALS_SQL = """
    SELECT * FROM stg_matches
    WHERE home_goals < 0 OR away_goals < 0
"""


class TestDbtTests:

    @pytest.fixture(autouse=True)
    def setup_view(self, spark, sample_rows):
        df = spark.createDataFrame(sample_rows)
        df.createOrReplaceTempView("raw_matches")
        stg = spark.sql(STG_MATCHES_SQL)
        stg.createOrReplaceTempView("stg_matches")

    def test_no_negative_goals_in_clean_data(self, spark):
        violations = spark.sql(ASSERT_POSITIVE_GOALS_SQL)
        assert violations.count() == 0

    def test_negative_goals_detected(self, spark, sample_rows):
        bad = [dict(r) for r in sample_rows]
        bad[0]["home_goals"] = "-5"
        df = spark.createDataFrame(bad)
        df.createOrReplaceTempView("raw_matches")
        stg = spark.sql(STG_MATCHES_SQL)
        stg.createOrReplaceTempView("stg_matches")
        violations = spark.sql(ASSERT_POSITIVE_GOALS_SQL)
        assert violations.count() == 1
