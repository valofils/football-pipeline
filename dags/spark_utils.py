"""
spark_utils.py
~~~~~~~~~~~~~~
Shared SparkSession factory, PyArrow schema, and JDBC helpers used by both
the DAG tasks and the test suite.

New concepts vs v3
------------------
SparkSession   — the single entry point to all Spark functionality; think of it
                 as the distributed equivalent of a psycopg2 connection.
master URL     — "local[*]" uses all local cores (dev/test); swap for
                 "spark://spark-master:7077" to target the Docker cluster.
JDBC           — Spark writes DataFrames to Postgres over the standard Java
                 database connectivity driver; the JAR must be on the classpath.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DateType,
)

# ── PyArrow-compatible schema reused from v1/v2 ──────────────────────────────

MATCH_SCHEMA = StructType([
    StructField("match_id",        StringType(),  nullable=False),
    StructField("date",            DateType(),    nullable=False),
    StructField("season",          StringType(),  nullable=False),
    StructField("home_team",       StringType(),  nullable=False),
    StructField("away_team",       StringType(),  nullable=False),
    StructField("home_goals",      IntegerType(), nullable=False),
    StructField("away_goals",      IntegerType(), nullable=False),
    StructField("home_shots",      IntegerType(), nullable=True),
    StructField("away_shots",      IntegerType(), nullable=True),
    StructField("home_possession", FloatType(),   nullable=True),
    StructField("away_possession", FloatType(),   nullable=True),
    StructField("stadium",         StringType(),  nullable=True),
    StructField("referee",         StringType(),  nullable=True),
])

# ── SparkSession factory ──────────────────────────────────────────────────────

def get_spark(app_name: str = "FootballPipeline", master: str | None = None) -> SparkSession:
    """
    Return (or reuse) a SparkSession.

    master defaults to the SPARK_MASTER_URL env var set in docker-compose,
    falling back to local[*] for unit tests that run outside Docker.

    The PostgreSQL JDBC JAR path comes from the SPARK_JARS env var; if it is
    not set the session still builds — useful in test environments where no
    real Postgres write happens.
    """
    resolved_master = master or os.getenv("SPARK_MASTER_URL", "local[*]")
    jar_path = os.getenv("SPARK_JARS", "")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(resolved_master)
        # Parquet: use legacy timestamp format so PyArrow files from v1 are
        # readable without schema evolution headaches.
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        # Snappy is already the v1 default; make it explicit here.
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    if jar_path:
        builder = builder.config("spark.jars", jar_path)

    return builder.getOrCreate()


# ── JDBC helpers ──────────────────────────────────────────────────────────────

def jdbc_url(host: str, port: int, db: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_properties(user: str, password: str) -> dict:
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def jdbc_params_from_env() -> tuple[str, dict]:
    """
    Build JDBC url + properties from env vars injected by docker-compose.
    Returns (url, properties) ready for df.write.jdbc(...).
    """
    host     = os.getenv("FOOTBALL_DB_HOST", "localhost")
    port     = int(os.getenv("FOOTBALL_DB_PORT", "5433"))
    db       = os.getenv("FOOTBALL_DB_NAME", "football")
    user     = os.getenv("FOOTBALL_DB_USER", "football")
    password = os.getenv("FOOTBALL_DB_PASS", "football")

    url   = jdbc_url(host, port, db)
    props = jdbc_properties(user, password)
    return url, props
