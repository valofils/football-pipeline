# football-pipeline · v11 — Spark Structured Streaming

Replaces the finite-batch Kafka consumer with a production-grade
**Spark Structured Streaming** job backed by Delta Lake merge semantics.

## What changed in v11

| Area | v10 | v11 |
|---|---|---|
| Kafka consumer | `PythonOperator` polling batch | `BashOperator` → `spark-submit --once` |
| Ingest file | `dags/kafka_consumer.py` | `streaming/stream_ingest.py` |
| DAG task | `kafka_ingest` | `streaming_ingest` |
| Delta write | `mode("append")` | `foreachBatch` + `DeltaTable.merge()` |
| Checkpoint | none | `s3a://…/checkpoints/matches` |
| New tests | — | Class 13 `TestStreamingIngest` (12 tests → 77 total) |

All v10 files (MLflow, XGBoost, Great Expectations, dbt, Terraform) are
unchanged.

## Architecture

```
Kafka topic: football_matches
        │
        ▼
spark-submit streaming/stream_ingest.py --once
        │  readStream (kafka format)
        │  from_json → MATCH_SCHEMA
        │  filter(match_id.isNotNull)
        │
        ▼  foreachBatch(_upsert_to_delta)
Delta Lake: s3a://football-data/delta/matches   ←── merge on match_id
        │
        ▼
Airflow DAG: streaming_ingest >> validate >> load_postgres >> [dbt_run, ml_train] >> predict
```

## Key design decisions

**`--once` trigger** — makes the streaming job finite so Airflow can track
success/failure as a normal task. In production, remove `--once` and run
the job as a long-lived service outside Airflow (or in a
`KubernetesPodOperator`).

**`foreachBatch` + Delta merge** — each micro-batch upserts on `match_id`,
making the job fully idempotent. Replaying the same Kafka partition range
is safe.

**Checkpoint location** — stored in S3 (`STREAMING_CHECKPOINT_PATH`).
Allows restarts without reprocessing already-seen offsets.

**`failOnDataLoss=false`** — prevents crashes on Kafka log compaction or
offset gaps in dev/test environments.

## Running locally

```bash
# 1. Start infrastructure
docker compose up -d airflow-db kafka zookeeper mlflow moto-server

# 2. Initialise Airflow DB + MLflow DB
docker compose run --rm airflow-init

# 3. Produce some test messages
python scripts/produce_test_messages.py --count 50

# 4. Run the streaming job manually (drains available offsets then exits)
KAFKA_STARTING_OFFSETS=earliest \
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/stream_ingest.py --once

# 5. Trigger the full DAG
docker compose up -d airflow-webserver airflow-scheduler
# Open http://localhost:8080 → football_pipeline → Trigger DAG
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker address |
| `KAFKA_TOPIC` | `football_matches` | Source topic |
| `KAFKA_STARTING_OFFSETS` | `latest` | `latest` / `earliest` |
| `DELTA_PATH` | `s3a://…/delta/matches` | Target Delta table |
| `STREAMING_CHECKPOINT_PATH` | `s3a://…/checkpoints/matches` | Checkpoint dir |
| `STREAMING_TRIGGER_INTERVAL` | `10 seconds` | processingTime trigger |
| `SPARK_SUBMIT_OPTIONS` | *(packages string)* | Extra spark-submit args |

## Tests

```bash
pytest tests/ -v --tb=short
# 77 tests, 13 classes
```

## Full stack

Python 3.12 · Kafka 3.7 · PySpark 3.5.1 · Delta Lake 3.2.0 ·
PostgreSQL 16 · AWS S3/RDS · dbt-core 1.8.3 · Great Expectations 0.18.19 ·
XGBoost 2.0.3 · scikit-learn 1.5.0 · MLflow 2.13.0 · Airflow 2.9.1 ·
Docker Compose · Terraform 1.7+
