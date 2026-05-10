"""
consume_matches.py
------------------
Consumes messages from the Kafka topic `match-events` and bulk-upserts
them into the PostgreSQL staging table `matches_staging`.

The consumer is designed to be called as a finite batch job from Airflow
(not a long-running daemon), so it:
  - Seeks to the stored committed offset (or earliest if no offset exists)
  - Reads until it catches up to the high-water mark (end of topic)
  - Commits offsets and exits

Environment variables (all optional, sensible defaults for Docker Compose):
    KAFKA_BOOTSTRAP_SERVERS   default: kafka:9092
    KAFKA_GROUP_ID            default: football-pipeline-consumer
    FOOTBALL_DB_HOST          default: postgres
    FOOTBALL_DB_PORT          default: 5432
    FOOTBALL_DB_NAME          default: football
    FOOTBALL_DB_USER          default: airflow
    FOOTBALL_DB_PASSWORD      default: airflow

Usage (standalone):
    python kafka/consumer/consume_matches.py
    python kafka/consumer/consume_matches.py --timeout 60 --batch-size 500
"""

import argparse
import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "match-events"

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def kafka_config() -> dict:
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "football-pipeline-consumer"),
        # Start from earliest unread message when no committed offset exists
        "auto.offset.reset": "earliest",
        # Disable auto-commit — we commit manually after successful DB write
        "enable.auto.commit": False,
        # Heartbeat / session timeouts tuned for batch processing
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
    }


def db_config() -> dict:
    return {
        "host": os.getenv("FOOTBALL_DB_HOST", "postgres"),
        "port": int(os.getenv("FOOTBALL_DB_PORT", "5432")),
        "dbname": os.getenv("FOOTBALL_DB_NAME", "football"),
        "user": os.getenv("FOOTBALL_DB_USER", "airflow"),
        "password": os.getenv("FOOTBALL_DB_PASSWORD", "airflow"),
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(**db_config())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


DDL_STAGING = """
CREATE TABLE IF NOT EXISTS matches_staging (
    match_id        INTEGER PRIMARY KEY,
    season          TEXT    NOT NULL,
    home_team       TEXT    NOT NULL,
    away_team       TEXT    NOT NULL,
    home_goals      INTEGER,
    away_goals      INTEGER,
    match_date      DATE,
    referee         TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
"""


def ensure_staging_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_STAGING)
    conn.commit()
    log.info("Staging table ready.")


UPSERT_SQL = """
INSERT INTO matches_staging (
    match_id, season, home_team, away_team,
    home_goals, away_goals, match_date, referee
)
VALUES %s
ON CONFLICT (match_id) DO UPDATE SET
    season      = EXCLUDED.season,
    home_team   = EXCLUDED.home_team,
    away_team   = EXCLUDED.away_team,
    home_goals  = EXCLUDED.home_goals,
    away_goals  = EXCLUDED.away_goals,
    match_date  = EXCLUDED.match_date,
    referee     = EXCLUDED.referee,
    ingested_at = NOW();
"""


def upsert_batch(conn: psycopg2.extensions.connection, rows: list[dict]) -> int:
    """Upsert a list of parsed message dicts. Returns number of rows upserted."""
    records = [
        (
            int(r["match_id"]),
            r["season"],
            r["home_team"],
            r["away_team"],
            _int_or_none(r.get("home_goals")),
            _int_or_none(r.get("away_goals")),
            r.get("match_date") or None,
            r.get("referee") or None,
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, records, page_size=500)
    conn.commit()
    return len(records)


def _int_or_none(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# High-water mark helper — tells us when to stop consuming
# ---------------------------------------------------------------------------

def get_high_water_marks(consumer: Consumer, topic: str) -> dict[int, int]:
    """Return {partition: high_water_offset} for all partitions of *topic*."""
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = list(metadata.topics[topic].partitions.keys())
    hwm = {}
    for pid in partitions:
        tp = TopicPartition(topic, pid)
        lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
        hwm[pid] = hi
    return hwm


# ---------------------------------------------------------------------------
# Main consume loop
# ---------------------------------------------------------------------------

def consume(timeout_seconds: int = 120, batch_size: int = 1000) -> int:
    """
    Consume all pending messages from *TOPIC*, upsert to Postgres, commit offsets.
    Returns total number of rows written.
    """
    consumer = Consumer(kafka_config())
    consumer.subscribe([TOPIC])

    total_written = 0
    buffer: list[dict] = []
    deadline = time.monotonic() + timeout_seconds

    # Give the consumer time to join the group and receive partition assignment
    # before we snapshot high-water marks.
    log.info("Subscribing to topic '%s' …", TOPIC)
    # Trigger assignment by polling once
    consumer.poll(timeout=5.0)

    hwm = get_high_water_marks(consumer, TOPIC)
    log.info("High-water marks: %s", hwm)

    # Track committed positions per partition so we know when we're caught up
    positions: dict[int, int] = {}

    def _caught_up() -> bool:
        assignment = consumer.assignment()
        if not assignment:
            return False
        for tp in assignment:
            pos = positions.get(tp.partition, 0)
            target = hwm.get(tp.partition, 0)
            if pos < target:
                return False
        return True

    with get_conn() as conn:
        ensure_staging_table(conn)

        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — check if we've caught up
                if hwm and _caught_up():
                    log.info("Caught up to high-water mark — stopping.")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of this partition
                    positions[msg.partition()] = msg.offset() + 1
                    if _caught_up():
                        log.info("All partitions at EOF — stopping.")
                        break
                    continue
                raise KafkaException(msg.error())

            # Parse and buffer
            try:
                row = json.loads(msg.value().decode("utf-8"))
                buffer.append(row)
                positions[msg.partition()] = msg.offset() + 1
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("Skipping malformed message offset=%d: %s", msg.offset(), exc)
                continue

            # Flush buffer when it reaches batch_size
            if len(buffer) >= batch_size:
                n = upsert_batch(conn, buffer)
                consumer.commit(asynchronous=False)
                total_written += n
                log.info("Upserted batch of %d rows (total so far: %d).", n, total_written)
                buffer.clear()

        # Flush remaining buffer
        if buffer:
            n = upsert_batch(conn, buffer)
            consumer.commit(asynchronous=False)
            total_written += n
            log.info("Upserted final batch of %d rows.", n)

    consumer.close()
    log.info("Consumer closed. Total rows written: %d", total_written)
    return total_written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consume match-events from Kafka into Postgres.")
    p.add_argument("--timeout", type=int, default=120, help="Max seconds to wait for messages (default 120).")
    p.add_argument("--batch-size", type=int, default=1000, help="DB upsert batch size (default 1000).")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    rows = consume(timeout_seconds=args.timeout, batch_size=args.batch_size)
    print(f"Done — {rows} rows written to matches_staging.")
