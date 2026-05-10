"""
spark_utils.py — SparkSession factory and helpers for football-pipeline-v8.

New in v8
---------
* Delta Lake extensions configured on the SparkSession builder.
* `delta_path()` builds the canonical s3a:// path for a Delta table.
* `write_delta()` / `merge_delta()` convenience wrappers used by DAG tasks.

Everything from v7 (S3A config, jdbc helpers) is preserved unchanged.
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

MATCH_SCHEMA = StructType(
    [
        StructField("match_id", IntegerType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("home_team", StringType(), nullable=False),
        StructField("away_team", StringType(), nullable=False),
        StructField("home_goals", IntegerType(), nullable=True),
        StructField("away_goals", IntegerType(), nullable=True),
        StructField("result", StringType(), nullable=True),
        StructField("xg_home", DoubleType(), nullable=True),
        StructField("xg_away", DoubleType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

_DELTA_PACKAGES = [
    "io.delta:delta-spark_2.12:3.2.0",
]


def get_spark(app_name: str = "FootballPipeline") -> SparkSession:
    """
    Build and return a SparkSession with:
    - Delta Lake extensions (v8 new)
    - S3A filesystem configured for AWS / moto
    - Hive metastore disabled (local metastore via derby)
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Delta Lake catalog + SQL extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # moto / localstack endpoint override (no-op when env var absent)
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("AWS_S3_ENDPOINT", "https://s3.amazonaws.com"),
        )
        # Silence noisy AWS SDK logging
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        # Executor / driver sizing for local Docker environment
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")
    )

    # configure_spark_with_delta_pip adds the Delta JARs to the classpath
    # when running in local mode without a pre-staged JAR (e.g. CI).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def s3a_path(key: str) -> str:
    """Return ``s3a://<bucket>/<key>``."""
    bucket = os.environ["S3_BUCKET_NAME"]
    return f"s3a://{bucket}/{key}"


def delta_path(table: str) -> str:
    """Return the canonical S3A path for a Delta table, e.g. ``delta/matches``."""
    return s3a_path(f"delta/{table}")


# ---------------------------------------------------------------------------
# Delta helpers
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame, table: str, mode: str = "append") -> None:
    """
    Write *df* to the Delta table at ``delta/<table>`` on S3.

    Parameters
    ----------
    df:    Spark DataFrame to write.
    table: Logical table name (no path prefix).
    mode:  ``"append"`` (default) or ``"overwrite"``.
    """
    (
        df.write.format("delta")
        .mode(mode)
        .partitionBy("season")
        .save(delta_path(table))
    )


def merge_delta(spark: SparkSession, new_df: DataFrame, table: str) -> None:
    """
    MERGE new_df into an existing Delta table (upsert by ``match_id``).

    If the table does not yet exist, falls back to a full write.
    This is the storage-layer equivalent of the v7 ``ON CONFLICT DO UPDATE``.
    """
    from delta.tables import DeltaTable  # local import — only available at runtime

    path = delta_path(table)

    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("existing")
            .merge(
                new_df.alias("incoming"),
                "existing.match_id = incoming.match_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        write_delta(new_df, table, mode="overwrite")


# ---------------------------------------------------------------------------
# JDBC helpers (unchanged from v7)
# ---------------------------------------------------------------------------

def jdbc_url(host: str | None = None, port: str | None = None, db: str | None = None) -> str:
    host = host or os.environ["FOOTBALL_DB_HOST"]
    port = port or os.environ.get("FOOTBALL_DB_PORT", "5432")
    db = db or os.environ.get("FOOTBALL_DB_NAME", "football")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_properties() -> dict[str, str]:
    return {
        "user": os.environ["FOOTBALL_DB_USER"],
        "password": os.environ["FOOTBALL_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def jdbc_params_from_env() -> tuple[str, dict[str, str]]:
    return jdbc_url(), jdbc_properties()
