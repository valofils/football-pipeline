"""
spark_utils.py
--------------
Shared Spark helpers — carried forward from v4/v5 unchanged.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Schema for raw CSV reads (v4 ingest task used this; v6 ingest is Kafka-based
# so this is only used in tests and the validate/load tasks via JDBC inference).
MATCH_SCHEMA = StructType([
    StructField("match_id",   IntegerType(), nullable=False),
    StructField("season",     StringType(),  nullable=False),
    StructField("home_team",  StringType(),  nullable=False),
    StructField("away_team",  StringType(),  nullable=False),
    StructField("home_goals", IntegerType(), nullable=True),
    StructField("away_goals", IntegerType(), nullable=True),
    StructField("match_date", DateType(),    nullable=True),
    StructField("referee",    StringType(),  nullable=True),
])


def get_spark(app_name: str = "football-pipeline") -> SparkSession:
    """Return (or create) a SparkSession configured for this project."""
    jars = os.getenv("SPARK_JARS", "/opt/spark/jars/postgresql.jar")
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def jdbc_url(host: str, port: str | int, dbname: str, **_) -> str:
    return f"jdbc:postgresql://{host}:{port}/{dbname}"


def jdbc_properties(user: str, password: str, **_) -> dict:
    return {"user": user, "password": password, "driver": "org.postgresql.Driver"}


def jdbc_params_from_env() -> dict:
    return {
        "host":     os.getenv("FOOTBALL_DB_HOST",     "postgres"),
        "port":     os.getenv("FOOTBALL_DB_PORT",     "5432"),
        "dbname":   os.getenv("FOOTBALL_DB_NAME",     "football"),
        "user":     os.getenv("FOOTBALL_DB_USER",     "airflow"),
        "password": os.getenv("FOOTBALL_DB_PASSWORD", "airflow"),
    }
