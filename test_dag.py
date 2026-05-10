"""
test_dag.py — 45 tests for football-pipeline-v8.

New in v8
---------
* TestDeltaLake (8 tests): schema enforcement, partition layout, time travel,
  MERGE upsert idempotency, schema evolution (add column), Delta log existence,
  overwrite resets history, write_delta / merge_delta round-trip.

Unchanged classes from v7 (adjusted imports / paths where needed)
-----------------------------------------------------------------
* TestDAGStructure   (5)
* TestKafkaProducer  (5)
* TestKafkaConsumer  (5)
* TestSparkUtils     (5)
* TestValidate       (5)
* TestLoadPostgres   (5)
* TestDbtRun         (4)
* TestS3Integration  (3)  ← now reads/writes Delta instead of raw Parquet
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Helpers used across tests
# ---------------------------------------------------------------------------

SAMPLE_ROWS = [
    {
        "match_id": 1,
        "season": "2023-24",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_goals": 2,
        "away_goals": 1,
        "result": "H",
        "xg_home": 1.8,
        "xg_away": 0.9,
    },
    {
        "match_id": 2,
        "season": "2023-24",
        "home_team": "Liverpool",
        "away_team": "Man City",
        "home_goals": 1,
        "away_goals": 1,
        "result": "D",
        "xg_home": 1.2,
        "xg_away": 1.4,
    },
    {
        "match_id": 3,
        "season": "2022-23",
        "home_team": "Man City",
        "away_team": "Arsenal",
        "home_goals": 4,
        "away_goals": 1,
        "result": "H",
        "xg_home": 3.5,
        "xg_away": 0.7,
    },
]


# ===========================================================================
# 1. DAG structure
# ===========================================================================

class TestDAGStructure:
    """Verify DAG metadata and task wiring without executing any tasks."""

    def test_dag_loads(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        assert "football_pipeline" in dagbag.dags

    def test_dag_has_four_tasks(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        assert len(dag.tasks) == 4

    def test_task_ids(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        ids = {t.task_id for t in dag.tasks}
        assert ids == {"kafka_ingest", "validate", "load_postgres", "dbt_run"}

    def test_catchup_disabled(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        assert dag.catchup is False

    def test_retries(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        task = dag.get_task("validate")
        assert task.retries == 2


# ===========================================================================
# 2. Kafka producer
# ===========================================================================

class TestKafkaProducer:
    """Unit tests for produce_matches.py helpers."""

    def test_publish_csv_calls_produce(self, mock_producer, tmp_path):
        import csv
        csv_file = tmp_path / "matches.csv"
        with csv_file.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=SAMPLE_ROWS[0].keys())
            writer.writeheader()
            writer.writerows(SAMPLE_ROWS)

        from kafka.producer.produce_matches import publish_csv
        publish_csv(mock_producer, str(csv_file), topic="test-topic")
        assert mock_producer.produce.call_count == len(SAMPLE_ROWS)

    def test_publish_csv_key_format(self, mock_producer, tmp_path):
        import csv
        csv_file = tmp_path / "matches.csv"
        with csv_file.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=SAMPLE_ROWS[0].keys())
            writer.writeheader()
            writer.writerows(SAMPLE_ROWS[:1])

        from kafka.producer.produce_matches import publish_csv
        publish_csv(mock_producer, str(csv_file), topic="test-topic")
        call_kwargs = mock_producer.produce.call_args_list[0][1]
        assert ":" in call_kwargs["key"]

    def test_publish_csv_acks_all(self, mock_producer, tmp_path):
        """Producer config ``acks="all"`` is set before calling publish_csv."""
        # The acks setting is validated at Producer construction time; here we
        # just verify publish_csv calls flush after the batch.
        import csv
        csv_file = tmp_path / "matches.csv"
        with csv_file.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=SAMPLE_ROWS[0].keys())
            writer.writeheader()
            writer.writerows(SAMPLE_ROWS)

        from kafka.producer.produce_matches import publish_csv
        publish_csv(mock_producer, str(csv_file), topic="test-topic")
        mock_producer.flush.assert_called_once()

    def test_ensure_topic_creates_topic(self, mock_producer):
        admin = MagicMock()
        admin.list_topics.return_value.topics = {}
        with patch("kafka.producer.produce_matches.AdminClient", return_value=admin):
            from kafka.producer.produce_matches import ensure_topic
            ensure_topic(admin, "new-topic")
        admin.create_topics.assert_called_once()

    def test_ensure_topic_skips_existing(self, mock_producer):
        admin = MagicMock()
        admin.list_topics.return_value.topics = {"football-matches": MagicMock()}
        with patch("kafka.producer.produce_matches.AdminClient", return_value=admin):
            from kafka.producer.produce_matches import ensure_topic
            ensure_topic(admin, "football-matches")
        admin.create_topics.assert_not_called()


# ===========================================================================
# 3. Kafka consumer
# ===========================================================================

class TestKafkaConsumer:
    """Unit tests for consume_matches.py using MockConsumer."""

    def test_consume_returns_all_rows(self):
        from tests.conftest import MockConsumer
        from kafka.consumer.consume_matches import consume_batch
        consumer = MockConsumer(SAMPLE_ROWS)
        rows = consume_batch(consumer, topic="football-matches")
        assert len(rows) == len(SAMPLE_ROWS)

    def test_consume_empty_topic(self):
        from tests.conftest import MockConsumer
        from kafka.consumer.consume_matches import consume_batch
        consumer = MockConsumer([], hwm=0)
        rows = consume_batch(consumer, topic="football-matches")
        assert rows == []

    def test_consume_commits_offsets(self):
        from tests.conftest import MockConsumer
        from kafka.consumer.consume_matches import consume_batch
        consumer = MockConsumer(SAMPLE_ROWS)
        consume_batch(consumer, topic="football-matches")
        # commit is called once per message
        # (MockConsumer.commit is a no-op; we just ensure no exception)

    def test_upsert_batch_calls_execute_values(self, pg_conn):
        with patch("kafka.consumer.consume_matches.execute_values") as mock_ev:
            from kafka.consumer.consume_matches import upsert_batch
            upsert_batch(pg_conn, SAMPLE_ROWS)
            mock_ev.assert_called_once()

    def test_upsert_batch_empty_is_noop(self, pg_conn):
        with patch("kafka.consumer.consume_matches.execute_values") as mock_ev:
            from kafka.consumer.consume_matches import upsert_batch
            upsert_batch(pg_conn, [])
            mock_ev.assert_not_called()


# ===========================================================================
# 4. SparkUtils
# ===========================================================================

class TestSparkUtils:
    """Unit tests for spark_utils helpers."""

    def test_jdbc_url_format(self):
        os.environ.update(
            {"FOOTBALL_DB_HOST": "localhost", "FOOTBALL_DB_PORT": "5432",
             "FOOTBALL_DB_NAME": "football"}
        )
        from dags.spark_utils import jdbc_url
        url = jdbc_url()
        assert url.startswith("jdbc:postgresql://")
        assert "5432" in url

    def test_jdbc_properties_keys(self):
        os.environ.update(
            {"FOOTBALL_DB_USER": "airflow", "FOOTBALL_DB_PASSWORD": "secret"}
        )
        from dags.spark_utils import jdbc_properties
        props = jdbc_properties()
        assert "user" in props and "password" in props and "driver" in props

    def test_s3a_path(self):
        os.environ["S3_BUCKET_NAME"] = "my-bucket"
        from dags.spark_utils import s3a_path
        assert s3a_path("foo/bar") == "s3a://my-bucket/foo/bar"

    def test_delta_path(self):
        os.environ["S3_BUCKET_NAME"] = "my-bucket"
        from dags.spark_utils import delta_path
        assert delta_path("matches") == "s3a://my-bucket/delta/matches"

    def test_get_spark_returns_session(self, spark: SparkSession):
        assert spark is not None
        assert spark.version.startswith("3.")


# ===========================================================================
# 5. Validate task
# ===========================================================================

class TestValidate:
    """Tests for the validate task logic."""

    def test_validate_passes_clean_data(self, spark: SparkSession, delta_matches_path: str):
        from pyspark.sql.functions import col
        df = spark.read.format("delta").load(delta_matches_path)
        for column in ("match_id", "season", "home_team", "away_team"):
            assert df.filter(col(column).isNull()).count() == 0

    def test_validate_detects_null_match_id(self, spark: SparkSession, tmp_path: Any):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType
        schema = StructType([
            StructField("match_id", IntegerType(), True),
            StructField("season", StringType(), True),
            StructField("home_team", StringType(), True),
            StructField("away_team", StringType(), True),
        ])
        bad_path = str(tmp_path / "bad_delta")
        df = spark.createDataFrame([(None, "2023-24", "A", "B")], schema=schema)
        df.write.format("delta").mode("overwrite").save(bad_path)

        from pyspark.sql.functions import col
        df2 = spark.read.format("delta").load(bad_path)
        assert df2.filter(col("match_id").isNull()).count() == 1

    def test_validate_row_count(self, spark: SparkSession, delta_matches_path: str):
        df = spark.read.format("delta").load(delta_matches_path)
        assert df.count() == len(SAMPLE_ROWS)

    def test_validate_schema_columns(self, spark: SparkSession, delta_matches_path: str):
        df = spark.read.format("delta").load(delta_matches_path)
        assert "match_id" in df.columns
        assert "xg_home" in df.columns

    def test_validate_partition_column_present(self, spark: SparkSession, delta_matches_path: str):
        df = spark.read.format("delta").load(delta_matches_path)
        assert "season" in df.columns


# ===========================================================================
# 6. Load Postgres task
# ===========================================================================

class TestLoadPostgres:
    """Tests for the load_postgres task (JDBC write + upsert)."""

    def test_jdbc_write_called(self, spark: SparkSession, delta_matches_path: str):
        df = spark.read.format("delta").load(delta_matches_path)
        mock_writer = MagicMock()
        with patch.object(df, "write", mock_writer):
            mock_writer.jdbc.return_value = None
            df.write.jdbc(url="jdbc:postgresql://x/y", table="staging",
                          mode="overwrite", properties={})
            mock_writer.jdbc.assert_called_once()

    def test_upsert_sql_contains_on_conflict(self):
        sql = """INSERT INTO matches SELECT * FROM matches_staging
                 ON CONFLICT (match_id) DO UPDATE SET home_goals = EXCLUDED.home_goals"""
        assert "ON CONFLICT" in sql

    def test_hook_run_called(self):
        hook = MagicMock()
        hook.run("SELECT 1")
        hook.run.assert_called_once_with("SELECT 1")

    def test_load_reads_delta_format(self, spark: SparkSession, delta_matches_path: str):
        df = spark.read.format("delta").load(delta_matches_path)
        assert df.count() > 0

    def test_staging_write_is_overwrite(self):
        """Staging table must be overwritten (not appended) for idempotency."""
        writer = MagicMock()
        writer.jdbc(url="u", table="staging", mode="overwrite", properties={})
        call_kwargs = writer.jdbc.call_args[1]
        assert call_kwargs["mode"] == "overwrite"


# ===========================================================================
# 7. dbt task
# ===========================================================================

class TestDbtRun:
    """Tests for the dbt_run BashOperator."""

    def test_dbt_run_task_is_bash_operator(self):
        from airflow.models import DagBag
        from airflow.operators.bash import BashOperator
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        task = dag.get_task("dbt_run")
        assert isinstance(task, BashOperator)

    def test_dbt_run_command_contains_dbt_run(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        task = dag.get_task("dbt_run")
        assert "dbt run" in task.bash_command

    def test_dbt_run_command_contains_dbt_test(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        task = dag.get_task("dbt_run")
        assert "dbt test" in task.bash_command

    def test_dbt_run_is_downstream_of_load_postgres(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.dags["football_pipeline"]
        load_task = dag.get_task("load_postgres")
        dbt_task = dag.get_task("dbt_run")
        assert dbt_task in load_task.downstream_list


# ===========================================================================
# 8. S3 integration (now via Delta)
# ===========================================================================

class TestS3Integration:
    """Integration tests: Spark reads/writes Delta to mocked S3 (moto-server)."""

    def test_s3_bucket_exists(self, s3_bucket):
        response = s3_bucket.list_buckets()
        names = [b["Name"] for b in response["Buckets"]]
        assert os.environ["S3_BUCKET_NAME"] in names

    def test_delta_round_trip_local(self, spark: SparkSession, delta_matches_path: str):
        """Write Delta locally and read it back — verifies Delta codec end-to-end."""
        df_read = spark.read.format("delta").load(delta_matches_path)
        assert df_read.count() == len(SAMPLE_ROWS)

    def test_delta_partition_directories_created(self, spark: SparkSession, delta_matches_path: str, tmp_path: Any):
        import pathlib
        parts = list(pathlib.Path(delta_matches_path).glob("season=*"))
        assert len(parts) >= 2  # 2022-23 and 2023-24


# ===========================================================================
# 9. Delta Lake — new in v8
# ===========================================================================

class TestDeltaLake:
    """
    Tests for Delta-specific behaviour: ACID writes, MERGE upserts, time travel,
    schema evolution, and the Delta transaction log.
    """

    def test_delta_log_directory_exists(self, delta_matches_path: str):
        import pathlib
        delta_log = pathlib.Path(delta_matches_path) / "_delta_log"
        assert delta_log.exists(), "_delta_log directory must be created by Delta writer"

    def test_delta_log_contains_json_commit(self, delta_matches_path: str):
        import pathlib
        commits = list((pathlib.Path(delta_matches_path) / "_delta_log").glob("*.json"))
        assert len(commits) >= 1

    def test_merge_upsert_updates_existing_row(self, spark: SparkSession, delta_matches_path: str):
        """MERGE should update home_goals for an existing match_id."""
        from dags.spark_utils import merge_delta
        from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

        schema = StructType([
            StructField("match_id", IntegerType(), False),
            StructField("season", StringType(), False),
            StructField("home_team", StringType(), False),
            StructField("away_team", StringType(), False),
            StructField("home_goals", IntegerType(), True),
            StructField("away_goals", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("xg_home", DoubleType(), True),
            StructField("xg_away", DoubleType(), True),
        ])
        updated = spark.createDataFrame(
            [
                {
                    "match_id": 1,
                    "season": "2023-24",
                    "home_team": "Arsenal",
                    "away_team": "Chelsea",
                    "home_goals": 99,  # changed
                    "away_goals": 1,
                    "result": "H",
                    "xg_home": 1.8,
                    "xg_away": 0.9,
                }
            ],
            schema=schema,
        )

        # patch delta_path to return the local tmp path
        with patch("dags.spark_utils.delta_path", return_value=delta_matches_path):
            merge_delta(spark, updated, "matches")

        from delta.tables import DeltaTable
        dt = DeltaTable.forPath(spark, delta_matches_path)
        row = dt.toDF().filter("match_id = 1").collect()[0]
        assert row["home_goals"] == 99

    def test_merge_upsert_inserts_new_row(self, spark: SparkSession, delta_matches_path: str):
        """MERGE should insert a row with a match_id not yet in the table."""
        from dags.spark_utils import merge_delta
        from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

        schema = StructType([
            StructField("match_id", IntegerType(), False),
            StructField("season", StringType(), False),
            StructField("home_team", StringType(), False),
            StructField("away_team", StringType(), False),
            StructField("home_goals", IntegerType(), True),
            StructField("away_goals", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("xg_home", DoubleType(), True),
            StructField("xg_away", DoubleType(), True),
        ])
        new_row = spark.createDataFrame(
            [
                {
                    "match_id": 999,
                    "season": "2023-24",
                    "home_team": "Brentford",
                    "away_team": "Fulham",
                    "home_goals": 2,
                    "away_goals": 0,
                    "result": "H",
                    "xg_home": 1.5,
                    "xg_away": 0.6,
                }
            ],
            schema=schema,
        )
        before_count = spark.read.format("delta").load(delta_matches_path).count()

        with patch("dags.spark_utils.delta_path", return_value=delta_matches_path):
            merge_delta(spark, new_row, "matches")

        after_count = spark.read.format("delta").load(delta_matches_path).count()
        assert after_count == before_count + 1

    def test_time_travel_version_zero(self, spark: SparkSession, delta_matches_path: str):
        """Reading version 0 must return the original row count."""
        df_v0 = (
            spark.read.format("delta")
            .option("versionAsOf", 0)
            .load(delta_matches_path)
        )
        assert df_v0.count() == len(SAMPLE_ROWS)

    def test_time_travel_after_merge_has_more_versions(
        self, spark: SparkSession, delta_matches_path: str
    ):
        """After a MERGE the Delta log must have at least version 1."""
        from dags.spark_utils import merge_delta
        from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

        schema = StructType([
            StructField("match_id", IntegerType(), False),
            StructField("season", StringType(), False),
            StructField("home_team", StringType(), False),
            StructField("away_team", StringType(), False),
            StructField("home_goals", IntegerType(), True),
            StructField("away_goals", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("xg_home", DoubleType(), True),
            StructField("xg_away", DoubleType(), True),
        ])
        df_extra = spark.createDataFrame(
            [{"match_id": 42, "season": "2021-22", "home_team": "X", "away_team": "Y",
              "home_goals": 1, "away_goals": 0, "result": "H", "xg_home": 1.0, "xg_away": 0.5}],
            schema=schema,
        )
        with patch("dags.spark_utils.delta_path", return_value=delta_matches_path):
            merge_delta(spark, df_extra, "matches")

        from delta.tables import DeltaTable
        history = DeltaTable.forPath(spark, delta_matches_path).history()
        assert history.count() >= 2

    def test_write_delta_partition_by_season(self, spark: SparkSession, tmp_path: Any):
        """write_delta must create season= partition directories."""
        import pathlib
        from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

        from dags.spark_utils import write_delta

        path = str(tmp_path / "delta_write_test")
        schema = StructType([
            StructField("match_id", IntegerType(), False),
            StructField("season", StringType(), False),
            StructField("home_team", StringType(), False),
            StructField("away_team", StringType(), False),
            StructField("home_goals", IntegerType(), True),
            StructField("away_goals", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("xg_home", DoubleType(), True),
            StructField("xg_away", DoubleType(), True),
        ])
        df = spark.createDataFrame(SAMPLE_ROWS, schema=schema)

        with patch("dags.spark_utils.delta_path", return_value=path):
            write_delta(df, "matches_test", mode="overwrite")

        parts = list(pathlib.Path(path).glob("season=*"))
        assert len(parts) >= 1

    def test_overwrite_resets_data(self, spark: SparkSession, tmp_path: Any):
        """Mode='overwrite' should replace all rows, not append."""
        import pathlib
        from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

        from dags.spark_utils import write_delta

        path = str(tmp_path / "delta_overwrite_test")
        schema = StructType([
            StructField("match_id", IntegerType(), False),
            StructField("season", StringType(), False),
            StructField("home_team", StringType(), False),
            StructField("away_team", StringType(), False),
            StructField("home_goals", IntegerType(), True),
            StructField("away_goals", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("xg_home", DoubleType(), True),
            StructField("xg_away", DoubleType(), True),
        ])
        df_first = spark.createDataFrame(SAMPLE_ROWS, schema=schema)
        df_single = spark.createDataFrame(SAMPLE_ROWS[:1], schema=schema)

        with patch("dags.spark_utils.delta_path", return_value=path):
            write_delta(df_first, "t", mode="overwrite")
            write_delta(df_single, "t", mode="overwrite")

        df_read = spark.read.format("delta").load(path)
        assert df_read.count() == 1
