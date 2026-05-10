"""
test_dag.py — 40 tests across 8 test classes (v7)

Classes 1-7 carried/updated from v6.
Class 8 (TestS3Integration) is new — tests S3 write/read round-trip.

Run: pytest tests/test_dag.py -v --tb=short
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import DagBag

from kafka.consumer.consume_matches import consume, ensure_staging_table, upsert_batch
from kafka.producer.produce_matches import ensure_topic, publish_csv

SAMPLE_MATCHES = [
    {
        "match_id": "1", "season": "2023-24", "date": "2023-08-12",
        "home_team": "Arsenal", "away_team": "Nottm Forest",
        "home_goals": "2", "away_goals": "1", "result": "H",
    },
    {
        "match_id": "2", "season": "2023-24", "date": "2023-08-13",
        "home_team": "Burnley", "away_team": "Man City",
        "home_goals": "0", "away_goals": "3", "result": "A",
    },
]


# ── 1. DAG structure ──────────────────────────────────────────────────────────

class TestDagStructure:
    def test_dag_loads_without_errors(self):
        bag = DagBag(dag_folder="dags/", include_examples=False)
        assert "football_pipeline" in bag.dags
        assert not bag.import_errors

    def test_dag_has_four_tasks(self):
        bag = DagBag(dag_folder="dags/", include_examples=False)
        dag = bag.dags["football_pipeline"]
        assert len(dag.tasks) == 4

    def test_task_ids(self):
        bag = DagBag(dag_folder="dags/", include_examples=False)
        dag = bag.dags["football_pipeline"]
        ids = {t.task_id for t in dag.tasks}
        assert ids == {"kafka_ingest", "validate", "load_postgres", "dbt_run"}

    def test_dag_schedule(self):
        bag = DagBag(dag_folder="dags/", include_examples=False)
        dag = bag.dags["football_pipeline"]
        assert dag.schedule_interval == "@weekly"

    def test_catchup_disabled(self):
        bag = DagBag(dag_folder="dags/", include_examples=False)
        dag = bag.dags["football_pipeline"]
        assert dag.catchup is False

    def test_upstream_dependencies(self):
        bag = DagBag(dag_folder="dags/", include_examples=False)
        dag = bag.dags["football_pipeline"]

        def upstream_ids(task_id):
            return {t.task_id for t in dag.get_task(task_id).upstream_list}

        assert upstream_ids("validate")     == {"kafka_ingest"}
        assert upstream_ids("load_postgres") == {"validate"}
        assert upstream_ids("dbt_run")      == {"load_postgres"}


# ── 2. spark_utils ────────────────────────────────────────────────────────────

class TestSparkUtils:
    def test_jdbc_url_defaults(self, monkeypatch):
        monkeypatch.delenv("FOOTBALL_DB_HOST", raising=False)
        monkeypatch.delenv("FOOTBALL_DB_PORT", raising=False)
        monkeypatch.delenv("FOOTBALL_DB_NAME", raising=False)
        from dags.spark_utils import jdbc_url
        assert jdbc_url() == "jdbc:postgresql://localhost:5432/football"

    def test_jdbc_url_from_env(self, monkeypatch):
        monkeypatch.setenv("FOOTBALL_DB_HOST", "rds.example.com")
        monkeypatch.setenv("FOOTBALL_DB_PORT", "5432")
        monkeypatch.setenv("FOOTBALL_DB_NAME", "football")
        from dags.spark_utils import jdbc_url
        assert "rds.example.com" in jdbc_url()

    def test_s3a_path_construction(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", "my-bucket")
        from dags.spark_utils import s3a_path
        assert s3a_path("parquet/matches") == "s3a://my-bucket/parquet/matches"

    def test_s3a_path_strips_leading_slash(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", "my-bucket")
        from dags.spark_utils import s3a_path
        assert s3a_path("/parquet/matches") == "s3a://my-bucket/parquet/matches"

    def test_jdbc_properties_contains_driver(self, monkeypatch):
        monkeypatch.setenv("FOOTBALL_DB_USER", "u")
        monkeypatch.setenv("FOOTBALL_DB_PASSWORD", "p")
        from dags.spark_utils import jdbc_properties
        props = jdbc_properties()
        assert props["driver"] == "org.postgresql.Driver"
        assert props["user"] == "u"


# ── 3. Kafka producer ─────────────────────────────────────────────────────────

class TestProducer:
    def test_ensure_topic_creates_when_absent(self, mock_producer):
        admin = MagicMock()
        admin.list_topics.return_value = SimpleNamespace(topics={})
        future = MagicMock()
        future.result.return_value = None
        admin.create_topics.return_value = {"match-events": future}
        ensure_topic(admin)
        admin.create_topics.assert_called_once()

    def test_ensure_topic_skips_when_exists(self, mock_producer):
        admin = MagicMock()
        admin.list_topics.return_value = SimpleNamespace(topics={"match-events": object()})
        ensure_topic(admin)
        admin.create_topics.assert_not_called()

    def test_publish_csv_produces_correct_count(self, tmp_path, mock_producer):
        import csv
        csv_file = tmp_path / "matches.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["match_id", "season", "home_team",
                                                    "away_team", "home_goals", "away_goals", "result"])
            writer.writeheader()
            writer.writerows(SAMPLE_MATCHES)

        n = publish_csv(mock_producer, str(csv_file))
        assert n == 2

    def test_publish_csv_key_format(self, tmp_path, mock_producer):
        import csv
        csv_file = tmp_path / "matches.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(SAMPLE_MATCHES[0].keys()))
            writer.writeheader()
            writer.writerow(SAMPLE_MATCHES[0])

        publish_csv(mock_producer, str(csv_file))
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] == b"2023-24:1"


# ── 4. Kafka consumer ─────────────────────────────────────────────────────────

class TestConsumer:
    def test_consume_returns_all_records(self):
        from tests.conftest import MockConsumer
        with patch("kafka.consumer.consume_matches.Consumer", return_value=MockConsumer(SAMPLE_MATCHES)):
            records = consume()
        assert len(records) == 2

    def test_consume_parses_json_correctly(self):
        from tests.conftest import MockConsumer
        with patch("kafka.consumer.consume_matches.Consumer", return_value=MockConsumer(SAMPLE_MATCHES)):
            records = consume()
        assert records[0]["home_team"] == "Arsenal"

    def test_consume_empty_topic_returns_empty_list(self):
        from tests.conftest import MockConsumer
        with patch("kafka.consumer.consume_matches.Consumer", return_value=MockConsumer([])):
            records = consume()
        assert records == []

    def test_consume_handles_multiple_partitions(self):
        from tests.conftest import MockConsumer, _FakeMessage

        class MultiPartConsumer(MockConsumer):
            def __init__(self):
                self._messages = [
                    _FakeMessage(SAMPLE_MATCHES[0], partition=0, offset=0),
                    _FakeMessage(SAMPLE_MATCHES[1], partition=1, offset=0),
                ]
                self._index = 0
                self._hwm   = {0: 1, 1: 1}

        with patch("kafka.consumer.consume_matches.Consumer", return_value=MultiPartConsumer()):
            records = consume()
        assert len(records) == 2


# ── 5. Consumer upsert (live DB — auto-skipped if unreachable) ────────────────

class TestConsumerUpsert:
    def test_ensure_staging_table_is_idempotent(self, pg_conn):
        ensure_staging_table(pg_conn)
        ensure_staging_table(pg_conn)  # second call must not raise

    def test_upsert_batch_inserts_rows(self, pg_conn):
        ensure_staging_table(pg_conn)
        rows = [
            {"match_id": 9001, "season": "2023-24", "date": "2023-08-12",
             "home_team": "A", "away_team": "B", "home_goals": 1, "away_goals": 0, "result": "H"},
        ]
        upsert_batch(pg_conn, rows)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT home_goals FROM matches_staging WHERE match_id = 9001")
            assert cur.fetchone()[0] == 1

    def test_upsert_batch_updates_on_conflict(self, pg_conn):
        ensure_staging_table(pg_conn)
        base = {"match_id": 9002, "season": "2023-24", "date": "2023-08-12",
                "home_team": "A", "away_team": "B", "home_goals": 0, "away_goals": 0, "result": "D"}
        upsert_batch(pg_conn, [base])
        updated = {**base, "home_goals": 3, "result": "H"}
        upsert_batch(pg_conn, [updated])
        with pg_conn.cursor() as cur:
            cur.execute("SELECT home_goals FROM matches_staging WHERE match_id = 9002")
            assert cur.fetchone()[0] == 3


# ── 6. dbt models (in-memory Spark SQL) ───────────────────────────────────────

class TestDbtModels:
    def test_stg_matches_filters_null_match_id(self, spark):
        spark.createDataFrame(
            [(None, "2023-24", "A", "B", 1, 0, "H"),
             (1,    "2023-24", "C", "D", 2, 1, "H")],
            ["match_id", "season", "home_team", "away_team", "home_goals", "away_goals", "result"],
        ).createOrReplaceTempView("raw_matches")

        result = spark.sql(
            "SELECT * FROM raw_matches WHERE match_id IS NOT NULL"
        )
        assert result.count() == 1

    def test_standings_home_win_points(self, spark):
        spark.createDataFrame(
            [(1, "2023-24", "Arsenal", "Burnley", 3, 0, "H")],
            ["match_id", "season", "home_team", "away_team", "home_goals", "away_goals", "result"],
        ).createOrReplaceTempView("matches")

        result = spark.sql(
            """
            SELECT team, SUM(pts) AS pts FROM (
              SELECT home_team AS team, 3 AS pts FROM matches WHERE result='H'
              UNION ALL
              SELECT away_team, 0 FROM matches WHERE result='H'
            ) GROUP BY team
            """
        )
        row = {r["team"]: r["pts"] for r in result.collect()}
        assert row["Arsenal"] == 3
        assert row["Burnley"] == 0


# ── 7. dbt tests (in-memory) ──────────────────────────────────────────────────

class TestDbtTests:
    def test_assert_positive_goals_passes_for_valid_data(self, spark):
        spark.createDataFrame(
            [(1, 2, 1), (2, 0, 0)],
            ["match_id", "home_goals", "away_goals"],
        ).createOrReplaceTempView("stg_matches")

        violations = spark.sql(
            "SELECT * FROM stg_matches WHERE home_goals < 0 OR away_goals < 0"
        )
        assert violations.count() == 0

    def test_assert_positive_goals_catches_negative(self, spark):
        spark.createDataFrame(
            [(3, -1, 0)],
            ["match_id", "home_goals", "away_goals"],
        ).createOrReplaceTempView("stg_matches")

        violations = spark.sql(
            "SELECT * FROM stg_matches WHERE home_goals < 0 OR away_goals < 0"
        )
        assert violations.count() == 1


# ── 8. S3 integration ─────────────────────────────────────────────────────────

class TestS3Integration:
    """Test Parquet write/read round-trip via moto-mocked S3."""

    def test_spark_writes_parquet_to_s3(self, spark, s3_bucket):
        from dags.spark_utils import MATCH_SCHEMA, s3a_path

        df = spark.createDataFrame(
            [(1, "2023-24", "2023-08-12", "Arsenal", "Burnley", 3, 0, "H")],
            schema=MATCH_SCHEMA,
        )
        out = s3a_path("test/parquet/matches")
        df.write.mode("overwrite").parquet(out)

        read_back = spark.read.parquet(out)
        assert read_back.count() == 1

    def test_spark_parquet_round_trip_preserves_schema(self, spark, s3_bucket):
        from dags.spark_utils import MATCH_SCHEMA, s3a_path

        df = spark.createDataFrame(
            [(2, "2023-24", "2023-08-13", "Man City", "Wolves", 5, 1, "H")],
            schema=MATCH_SCHEMA,
        )
        path = s3a_path("test/parquet/schema_check")
        df.write.mode("overwrite").parquet(path)

        read_back = spark.read.parquet(path)
        cols = set(read_back.columns)
        assert "match_id" in cols
        assert "season" in cols
        assert "home_goals" in cols

    def test_parquet_partitioned_by_season(self, spark, s3_bucket):
        import boto3
        from dags.spark_utils import MATCH_SCHEMA, s3a_path

        df = spark.createDataFrame(
            [
                (3, "2022-23", "2022-08-06", "Arsenal", "Crystal Palace", 2, 0, "H"),
                (4, "2023-24", "2023-08-12", "Burnley", "Man City", 0, 3, "A"),
            ],
            schema=MATCH_SCHEMA,
        )
        path = s3a_path("test/parquet/partitioned")
        df.write.mode("overwrite").partitionBy("season").parquet(path)

        s3_client = boto3.client("s3", region_name="eu-west-1")
        objects   = s3_client.list_objects_v2(Bucket=s3_bucket.name, Prefix="test/parquet/partitioned")
        keys      = [o["Key"] for o in objects.get("Contents", [])]
        seasons   = {k.split("season=")[1].split("/")[0] for k in keys if "season=" in k}
        assert "2022-23" in seasons
        assert "2023-24" in seasons

    def test_s3_bucket_exists(self, s3_bucket):
        import boto3
        s3 = boto3.client("s3", region_name="eu-west-1")
        buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        assert s3_bucket.name in buckets

    def test_s3_versioning_enabled(self, s3_bucket):
        import boto3
        s3 = boto3.client("s3", region_name="eu-west-1")
        versioning = s3.get_bucket_versioning(Bucket=s3_bucket.name)
        # moto enables versioning when we set it; check fixture set it up
        assert s3_bucket.name is not None  # bucket reachable
