"""
spark_utils.py — shared Spark helpers (identical to v4).
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

MATCH_SCHEMA = StructType(
    [
        StructField("match_id", StringType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("matchday", IntegerType(), nullable=False),
        StructField("home_team", StringType(), nullable=False),
        StructField("away_team", StringType(), nullable=False),
        StructField("home_goals", IntegerType(), nullable=False),
        StructField("away_goals", IntegerType(), nullable=False),
    ]
)


def get_spark(app_name: str = "football-pipeline") -> SparkSession:
    """Return (or create) a SparkSession wired to the cluster / local mode."""
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    jars = os.getenv("SPARK_JARS", "")

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    )
    if jars:
        builder = builder.config("spark.jars", jars)

    return builder.getOrCreate()


def jdbc_url(host: str, port: str | int, dbname: str, **_) -> str:
    return f"jdbc:postgresql://{host}:{port}/{dbname}"


def jdbc_properties(user: str, password: str, **_) -> dict[str, str]:
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def jdbc_params_from_env() -> dict[str, str]:
    return {
        "host": os.getenv("FOOTBALL_DB_HOST", "localhost"),
        "port": os.getenv("FOOTBALL_DB_PORT", "5432"),
        "dbname": os.getenv("FOOTBALL_DB_NAME", "football"),
        "user": os.getenv("FOOTBALL_DB_USER", "football"),
        "password": os.getenv("FOOTBALL_DB_PASS", "football"),
    }
