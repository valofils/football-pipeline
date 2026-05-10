"""
produce_matches.py
------------------
Reads Premier League match CSVs from data/raw/ and publishes
one JSON message per match row to the Kafka topic `match-events`.

Usage:
    python kafka/producer/produce_matches.py --data-dir data/raw/
    python kafka/producer/produce_matches.py --data-dir data/raw/ --season 2023-24
"""

import argparse
import csv
import json
import logging
import os
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "match-events"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------

def ensure_topic(bootstrap_servers: str, topic: str, num_partitions: int = 3, replication_factor: int = 1) -> None:
    """Create topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        log.info("Topic '%s' already exists — skipping creation.", topic)
        return

    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config={
            # Keep messages for 7 days
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
            # Compact + delete: retain latest per key, expire old segments
            "cleanup.policy": "delete",
        },
    )
    futures = admin.create_topics([new_topic])
    for t, future in futures.items():
        try:
            future.result()
            log.info("Topic '%s' created (partitions=%d).", t, num_partitions)
        except Exception as exc:
            log.error("Failed to create topic '%s': %s", t, exc)
            raise


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def delivery_report(err, msg) -> None:
    """Called once per message by the producer after broker acknowledgement."""
    if err:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        log.debug(
            "Delivered key=%s to %s [partition %d] @ offset %d",
            msg.key().decode(),
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ---------------------------------------------------------------------------
# CSV → Kafka
# ---------------------------------------------------------------------------

def publish_csv(producer: Producer, csv_path: Path, season: str) -> int:
    """Publish all rows in *csv_path* as JSON messages. Returns message count."""
    count = 0
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Attach season so consumers can partition/filter without peeking payload
            row["season"] = season

            # Message key = season + match_id for deterministic partition routing
            match_id = row.get("match_id") or row.get("MatchID") or str(count)
            key = f"{season}:{match_id}"

            producer.produce(
                topic=TOPIC,
                key=key.encode(),
                value=json.dumps(row).encode(),
                on_delivery=delivery_report,
            )
            count += 1

            # Poll periodically to trigger delivery callbacks and prevent buffer overflow
            if count % 100 == 0:
                producer.poll(0)
                log.info("Queued %d messages from %s …", count, csv_path.name)

    return count


def run(data_dir: Path, season_filter: str | None) -> None:
    ensure_topic(BOOTSTRAP_SERVERS, TOPIC)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # Wait for leader + all in-sync replicas to ack
            "acks": "all",
            # Retry transient errors up to 5 times
            "retries": 5,
            "retry.backoff.ms": 300,
            # Batch small messages together for throughput
            "linger.ms": 10,
            "batch.size": 16384,
        }
    )

    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        log.warning("No CSV files found in %s", data_dir)
        return

    total = 0
    for csv_path in csv_files:
        # Derive season from filename: "2023-24.csv" → "2023-24"
        season = csv_path.stem
        if season_filter and season != season_filter:
            continue
        log.info("Publishing %s (season=%s) …", csv_path.name, season)
        n = publish_csv(producer, csv_path, season)
        total += n
        log.info("  → %d messages queued.", n)

    # Block until all queued messages are delivered
    log.info("Flushing %d total messages to broker …", total)
    remaining = producer.flush(timeout=30)
    if remaining:
        log.error("%d messages were NOT delivered within timeout.", remaining)
        raise RuntimeError("Producer flush timed out.")
    log.info("All %d messages delivered successfully.", total)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish match CSVs to Kafka.")
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"), help="Directory containing season CSVs.")
    parser.add_argument("--season", default=None, help="Only publish a specific season (e.g. 2023-24).")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args.data_dir, args.season)
