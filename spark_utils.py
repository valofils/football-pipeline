"""
spark_utils.py — SparkSession factory and shared helpers.

v7 changes vs v6:
- get_spark() accepts an optional s3_endpoint override (for local MinIO / testing)
- s3a_path() helper builds s3a:// URIs from env
- JDBC helpers unchanged
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Schema ────────────────────────────────────────────────────────────────────

MATCH_SCHEMA = StructType(
    [
        StructField("match_id", IntegerType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("date", StringType(), nullable=True),
        StructField("home_team", StringType(), nullable=True),
        StructField("away_team", StringType(), nullable=True),
        StructField("home_goals", IntegerType(), nullable=True),
        StructField("away_goals", IntegerType(), nullable=True),
        StructField("result", StringType(), nullable=True),
    ]
)

# ── SparkSession ──────────────────────────────────────────────────────────────

def get_spark(app_name: str = "FootballPipeline", s3_endpoint: str | None = None) -> SparkSession:
    """
    Build (or reuse) a SparkSession configured for:
    - S3A access via hadoop-aws (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from env)
    - JDBC PostgreSQL access
    - Optional endpoint override for local MinIO / test mocking
    """
    aws_key    = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    spark_jars = os.environ.get("SPARK_JARS", "")

    builder = (
        SparkSession.builder.appName(app_name)
        # ── S3A (hadoop-aws) ──────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key",           aws_key)
        .config("spark.hadoop.fs.s3a.secret.key",           aws_secret)
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        # ── Performance ───────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.fast.upload",          "true")
        .config("spark.hadoop.fs.s3a.multipart.size",       "104857600")  # 100 MB
        # ── UI / Logging ──────────────────────────────────────────────────
        .config("spark.ui.enabled",                         "false")
    )

    if spark_jars:
        builder = builder.config("spark.jars", spark_jars)

    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)

    return builder.getOrCreate()


# ── S3 helpers ────────────────────────────────────────────────────────────────

def s3a_path(key: str) -> str:
    """Build an s3a:// URI from S3_BUCKET_NAME env var."""
    bucket = os.environ["S3_BUCKET_NAME"]
    return f"s3a://{bucket}/{key.lstrip('/')}"


# ── JDBC helpers ──────────────────────────────────────────────────────────────

def jdbc_url() -> str:
    host = os.environ.get("FOOTBALL_DB_HOST", "localhost")
    port = os.environ.get("FOOTBALL_DB_PORT", "5432")
    db   = os.environ.get("FOOTBALL_DB_NAME", "football")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_properties() -> dict:
    return {
        "user":   os.environ.get("FOOTBALL_DB_USER", "football_user"),
        "password": os.environ.get("FOOTBALL_DB_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }


def jdbc_params_from_env() -> tuple[str, dict]:
    """Convenience: return (url, properties) together."""
    return jdbc_url(), jdbc_properties()
